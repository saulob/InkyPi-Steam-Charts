from plugins.base_plugin.base_plugin import BasePlugin
from utils.http_client import get_http_session
import base64
import concurrent.futures
from functools import lru_cache
import requests
from datetime import datetime
import logging
import html
import re
import threading
import time

logger = logging.getLogger(__name__)

STEAMCHARTS_HOME_URL = "https://steamcharts.com"
STEAMCHARTS_CHART_URL = "https://steamcharts.com/app/{appid}/chart-data.json"
STEAM_CAPSULE_URL = "https://cdn.akamai.steamstatic.com/steam/apps/{appid}/capsule_sm_120.jpg"
STEAM_CAPSULE_TIMEOUT = 15
STEAM_CAPSULE_CACHE_SIZE = 128
STEAMCHARTS_CHART_TIMEOUT = 30
STEAMCHARTS_REQUESTS_PER_SECOND = 2

LEGACY_MODE_ALIASES = {
    "top_sellers": "most_played",
}

CHART_MODES = {
    "new_trending": {
        "label": "Trending",
        "source": "steamcharts_trending",
        "table_variant": "trending",
    },
    "most_played": {
        "label": "Most Played",
        "source": "steamcharts_top_games",
        "table_variant": "top_games",
    },
    "top_records": {
        "label": "Top Records",
        "source": "steamcharts_top_records",
        "table_variant": "top_records",
    },
}

MAX_ITEMS = 5


class _RateLimiter:
    def __init__(self, requests_per_second):
        self._interval = 1 / requests_per_second
        self._lock = threading.Lock()
        self._next_request_at = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            if now < self._next_request_at:
                time.sleep(self._next_request_at - now)
                now = self._next_request_at
            self._next_request_at = now + self._interval


steamcharts_rate_limiter = _RateLimiter(STEAMCHARTS_REQUESTS_PER_SECOND)


class SteamCharts(BasePlugin):
    def generate_settings_template(self):
        template_params = super().generate_settings_template()
        template_params["chart_modes"] = CHART_MODES
        return template_params

    def generate_image(self, settings, device_config):
        mode = settings.get("mode", "new_trending")
        mode = LEGACY_MODE_ALIASES.get(mode, mode)
        raw_items_count = settings.get("itemsCount", MAX_ITEMS)
        try:
            items_count = int(raw_items_count)
        except (TypeError, ValueError):
            items_count = MAX_ITEMS
        items_count = max(1, min(items_count, MAX_ITEMS))
        show_images = str(settings.get("showImages", "true")).lower() == "true"

        mode_config = CHART_MODES.get(mode)
        if not mode_config:
            raise RuntimeError(f"Unknown chart mode: {mode}")

        games = self._fetch_games(mode_config["source"], items_count)
        if show_images:
            self._apply_cached_images(games)

        dimensions = device_config.get_resolution()
        if device_config.get_config("orientation") == "vertical":
            dimensions = dimensions[::-1]

        template_params = {
            "title": "STEAM CHARTS",
            "subtitle": mode_config["label"],
            "table_variant": mode_config["table_variant"],
            "games": games,
            "show_images": show_images,
            "plugin_settings": settings,
        }

        return self.render_image(
            dimensions, "steam_charts.html", "steam_charts.css", template_params
        )

    def _fetch_games(self, source, count):
        """Fetch a homepage section and enrich it with chart data when needed."""
        if source == "steamcharts_trending":
            games = self._scrape_steamcharts_trending(count)
            chart_data = self._fetch_chart_data_batch(
                [g["app_id"] for g in games], sparkline_hours=48, include_change=True
            )
        elif source == "steamcharts_top_games":
            games = self._scrape_steamcharts_top_games(count)
            chart_data = self._fetch_chart_data_batch(
                [g["app_id"] for g in games], sparkline_hours=30 * 24
            )
        elif source == "steamcharts_top_records":
            games = self._scrape_steamcharts_top_records(count)
            chart_data = self._fetch_chart_data_batch(
                [g["app_id"] for g in games], sparkline_hours=48
            )
        else:
            raise RuntimeError(f"Unknown chart source: {source}")

        for game in games:
            app_id = game["app_id"]
            stats = chart_data.get(app_id, {})
            game["sparkline_svg"] = stats.get("sparkline_svg")
            if source == "steamcharts_trending" and "change_24h_fmt" not in game:
                game["change_24h_fmt"] = self._format_change(stats.get("change_24h"))
            if source in {"steamcharts_trending", "steamcharts_top_games"} and "current_players_fmt" not in game:
                game["current_players_fmt"] = self._format_count(
                    stats.get("current_players")
                )

        return games

    def _apply_cached_images(self, games):
        for game in games:
            app_id = game.get("app_id")
            if app_id is None:
                continue
            try:
                game["image"] = self._get_cached_capsule_image(app_id)
            except Exception as e:
                logger.warning(f"Failed to cache capsule image for app {app_id}: {e}")
                # Clear the image field so templates do not leave a remote CDN URL.
                # This prevents Chromium (used by `take_screenshot`) from
                # performing uncontrolled network fetches outside our timeouts
                # and rate limits which can hang or slow rendering.
                game["image"] = ""

    def _fetch_homepage(self, failure_message):
        """Return SteamCharts homepage HTML or raise a descriptive runtime error."""
        try:
            steamcharts_rate_limiter.wait()
            session = get_http_session()
            resp = session.get(STEAMCHARTS_HOME_URL, timeout=15)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.error(f"Failed to fetch SteamCharts homepage: {e}")
            raise RuntimeError(f"{failure_message}: {e}") from e

    @staticmethod
    def _extract_table_rows(page_html, table_id, missing_message):
        """Extract table rows from a specific homepage table id."""
        table_match = re.search(
            rf'<table[^>]*id="{re.escape(table_id)}"[^>]*>.*?</table>',
            page_html,
            re.DOTALL,
        )
        if not table_match:
            raise RuntimeError(missing_message)
        return re.findall(r"<tr[^>]*>.*?</tr>", table_match.group(0), re.DOTALL)

    @staticmethod
    def _extract_app_id(row):
        appid_match = re.search(r"/app/(\d+)", row)
        if not appid_match:
            return None
        return int(appid_match.group(1))

    @staticmethod
    def _clean_cells(row):
        tds = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        return [
            re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", td)).strip()
            for td in tds
        ]

    def _scrape_steamcharts_trending(self, count):
        """Scrape the Trending section from steamcharts.com homepage."""
        homepage_html = self._fetch_homepage(
            "Unable to fetch Steam trending data. Please try again later."
        )
        rows = self._extract_table_rows(
            homepage_html,
            "trending-recent",
            "Trending section not found on steamcharts.com.",
        )

        games = []
        for row in rows:
            app_id = self._extract_app_id(row)
            if app_id is None:
                continue
            tds_clean = self._clean_cells(row)
            if len(tds_clean) < 4:
                continue

            name = html.unescape(tds_clean[0])
            change_fmt = html.unescape(tds_clean[1])
            players_raw = tds_clean[3]

            try:
                players_int = int(players_raw.replace(",", ""))
                players_fmt = self._format_count(players_int)
            except ValueError:
                players_fmt = "--"

            games.append({
                "rank": len(games) + 1,
                "app_id": app_id,
                "name": name,
                "image": STEAM_CAPSULE_URL.format(appid=app_id),
                "change_24h_fmt": change_fmt,
                "current_players_fmt": players_fmt,
            })
            if len(games) >= count:
                break

        if not games:
            raise RuntimeError("No trending games found on steamcharts.com.")

        return games

    def _scrape_steamcharts_top_games(self, count):
        """Scrape the Top Games By Current Players section from the homepage."""
        homepage_html = self._fetch_homepage(
            "Unable to fetch Steam top games data. Please try again later."
        )
        rows = self._extract_table_rows(
            homepage_html,
            "top-games",
            "Top games section not found on steamcharts.com.",
        )

        games = []
        for row in rows:
            app_id = self._extract_app_id(row)
            if app_id is None:
                continue
            tds_clean = self._clean_cells(row)
            if len(tds_clean) < 6:
                continue

            name = html.unescape(tds_clean[1])
            try:
                players_int = int(tds_clean[2].replace(",", ""))
                players_fmt = self._format_count(players_int)
            except ValueError:
                players_fmt = "--"

            try:
                peak_players_int = int(tds_clean[4].replace(",", ""))
                peak_players_fmt = self._format_count(peak_players_int)
            except ValueError:
                peak_players_fmt = "--"

            games.append({
                "rank": len(games) + 1,
                "app_id": app_id,
                "name": name,
                "image": STEAM_CAPSULE_URL.format(appid=app_id),
                "current_players_fmt": players_fmt,
                "peak_players_fmt": peak_players_fmt,
            })
            if len(games) >= count:
                break

        if not games:
            raise RuntimeError("No top games found on steamcharts.com.")

        return games

    def _scrape_steamcharts_top_records(self, count):
        """Scrape the Top Records section from the homepage."""
        homepage_html = self._fetch_homepage(
            "Unable to fetch Steam top records data. Please try again later."
        )
        rows = self._extract_table_rows(
            homepage_html,
            "toppeaks",
            "Top records section not found on steamcharts.com.",
        )

        games = []
        for row in rows:
            app_id = self._extract_app_id(row)
            if app_id is None:
                continue
            tds_clean = self._clean_cells(row)
            if len(tds_clean) < 4:
                continue

            name = html.unescape(tds_clean[0])
            try:
                peak_players_int = int(tds_clean[1].replace(",", ""))
                peak_players_fmt = self._format_count(peak_players_int)
            except ValueError:
                peak_players_fmt = "--"

            games.append({
                "rank": len(games) + 1,
                "app_id": app_id,
                "name": name,
                "image": STEAM_CAPSULE_URL.format(appid=app_id),
                "peak_players_fmt": peak_players_fmt,
                "peak_time_fmt": self._format_peak_time(tds_clean[2]),
            })
            if len(games) >= count:
                break

        if not games:
            raise RuntimeError("No top records found on steamcharts.com.")

        return games

    def _fetch_chart_data_batch(self, app_ids, sparkline_hours=48, include_change=False):
        """Fetch chart data for multiple games in parallel with a mode-specific window."""
        results = {}

        def fetch_one(app_id):
            return app_id, self._fetch_chart_stats(app_id, sparkline_hours, include_change)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_one, aid): aid for aid in app_ids}
            for future in concurrent.futures.as_completed(futures):
                try:
                    aid, stats = future.result()
                    results[aid] = stats
                except Exception as e:
                    aid = futures[future]
                    logger.warning(f"Chart data fetch failed for app {aid}: {e}")
                    results[aid] = {}

        return results

    @staticmethod
    @lru_cache(maxsize=STEAM_CAPSULE_CACHE_SIZE)
    def _get_cached_capsule_image(app_id):
        session = get_http_session()
        resp = session.get(
            STEAM_CAPSULE_URL.format(appid=app_id), timeout=STEAM_CAPSULE_TIMEOUT
        )
        resp.raise_for_status()
        encoded_image = base64.b64encode(resp.content).decode("ascii")
        return f"data:image/jpeg;base64,{encoded_image}"

    def _fetch_chart_stats(self, app_id, sparkline_hours=48, include_change=True):
        """Fetch chart data and compute a sparkline window plus optional 24h change."""
        try:
            url = STEAMCHARTS_CHART_URL.format(appid=app_id)
            steamcharts_rate_limiter.wait()
            # Use a per-call session to avoid sharing a requests.Session() across threads.
            # Shared sessions from `get_http_session()` are a global singleton and
            # may not be safe to reuse concurrently from multiple worker threads.
            # To retain retry and connection-pool characteristics, configure the
            # per-call session with the same HTTPAdapter settings used by
            # `get_http_session()` so transient failures are retried.
            with requests.Session() as session:
                session.headers.update({
                    'User-Agent': 'InkyPi/1.0 (https://github.com/fatihak/InkyPi/)'
                })
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=10,
                    pool_maxsize=10,
                    max_retries=3,
                    pool_block=False,
                )
                session.mount('http://', adapter)
                session.mount('https://', adapter)
                resp = session.get(url, timeout=STEAMCHARTS_CHART_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            logger.warning(f"Failed chart data for app {app_id}: {e}")
            return {}

        if not data:
            return {}

        # Anchor calculations to the newest datapoint timestamp instead of wall-clock
        # time. SteamCharts data timestamps can lag behind wall-clock time which
        # would skew the sparkline window and 24h comparison.
        latest_ts = data[-1][0]
        cutoff_window_ms = latest_ts - sparkline_hours * 3600 * 1000

        recent_window = [p for p in data if p[0] >= cutoff_window_ms]

        current_players = recent_window[-1][1] if recent_window else data[-1][1]

        change_24h = None
        if include_change and len(data) >= 2:
            cutoff_24h_ms = latest_ts - 24 * 3600 * 1000
            target_24h = min(data, key=lambda p: abs(p[0] - cutoff_24h_ms))
            if target_24h[1] > 0:
                change_24h = ((current_players - target_24h[1]) / target_24h[1]) * 100

        sparkline_svg = self._generate_sparkline_svg(recent_window)

        return {
            "current_players": current_players,
            "change_24h": change_24h,
            "sparkline_svg": sparkline_svg,
        }

    @staticmethod
    def _generate_sparkline_svg(data_points, width=120, height=30):
        """
        Generate inline SVG polyline string from [[timestamp_ms, count]] pairs.
        Includes downsampling, smoothing, and normalization for e-paper.
        """
        if not data_points or len(data_points) < 2:
            return None

        # 1. Downsample to ~24 points max
        target_points = 24
        if len(data_points) > target_points:
            indices = [int(i * (len(data_points) - 1) / (target_points - 1)) for i in range(target_points)]
            data_points = [data_points[i] for i in indices]

        counts = [p[1] for p in data_points]

        # 2. Simple Moving Average (window=5) to smooth the line further
        if len(counts) > 5:
            smoothed = []
            for i in range(len(counts)):
                # Window of 5: [i-2, i-1, i, i+1, i+2]
                window = counts[max(0, i-2):min(len(counts), i+3)]
                smoothed.append(sum(window) / len(window))
            counts = smoothed

        min_c, max_c = min(counts), max(counts)

        # 3. Handle flat line or very small variation
        if max_c == min_c or (max_c - min_c) < (max_c * 0.001):
            y = height / 2
            return f'<polyline points="0,{y} {width},{y}" />'

        # 4. Normalize vertical range with a larger margin (15%) to avoid extreme spikes
        # This keeps the variation readable and prevents touching top/bottom
        range_c = max_c - min_c
        margin = range_c * 0.15
        plot_min = min_c - margin
        plot_max = max_c + margin
        plot_range = plot_max - plot_min

        points = []
        for i, c in enumerate(counts):
            x = (i / (len(counts) - 1)) * width
            # Invert Y for SVG (0 is top)
            # Ensure we stay within [1, height-1] to account for stroke width
            y = (height - 2) - ((c - plot_min) / plot_range) * (height - 4) + 1
            points.append(f"{x:.1f},{y:.1f}")

        return '<polyline points="{}" />'.format(" ".join(points))

    @staticmethod
    def _format_count(count):
        """Format player count with thousands separator."""
        if count is None:
            return "--"
        return f"{count:,}"

    @staticmethod
    def _format_change(change):
        """Format 24h change as signed percentage."""
        if change is None:
            return "--"
        sign = "+" if change >= 0 else ""
        return f"{sign}{change:.1f}%"

    @staticmethod
    def _format_peak_time(raw_value):
        """Format SteamCharts Top Records timestamps like 'Aug 2024'."""
        if not raw_value:
            return "--"
        try:
            return datetime.strptime(raw_value, "%Y-%m-%dT%H:%M:%SZ").strftime("%b %Y")
        except ValueError:
            return raw_value
