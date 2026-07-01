"""抓取监控 HTTP 服务"""
import json
import logging
import os
import time

from aiohttp import web

from .pipeline import PipelineStats

logger = logging.getLogger(__name__)

def _template_dir() -> str:
    """返回 templates 目录的绝对路径"""
    # monitor.py 位于 crawler/monitor.py，templates 位于项目根目录
    this_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(this_dir, "..", "templates"))


TEMPLATE_DIR = _template_dir()


class MonitorServer:
    """aiohttp 监控服务器，与爬虫共享 event loop"""

    def __init__(self, stats: PipelineStats, config: dict):
        self.stats = stats
        self.host = config.get("host", "0.0.0.0")
        self.port = config.get("port", 8888)
        self.refresh_interval = config.get("refresh_interval", 3)
        self._runner: web.AppRunner | None = None

    async def start(self):
        app = web.Application()
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/api/stats", self._handle_stats)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info(f"监控页面: http://{self.host}:{self.port}")

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    async def _handle_index(self, request: web.Request) -> web.Response:
        """返回看板 HTML"""
        html_path = os.path.join(TEMPLATE_DIR, "dashboard.html")
        if not os.path.exists(html_path):
            logger.error(f"dashboard.html 未找到: {html_path}")
            return web.Response(text=f"dashboard.html 未找到: {html_path}", status=500)
        try:
            with open(html_path, encoding="utf-8") as f:
                html = f.read()
            html = html.replace("{{REFRESH_INTERVAL}}", str(self.refresh_interval * 1000))
            return web.Response(text=html, content_type="text/html")
        except Exception as e:
            logger.error(f"读取 dashboard.html 失败: {e}")
            return web.Response(text=f"读取 dashboard.html 失败: {e}", status=500)

    async def _handle_stats(self, request: web.Request) -> web.Response:
        """返回实时统计 JSON"""
        data = {
            "status": self.stats.status,
            "total_pending": self.stats.total_pending,
            "processed": self.stats.processed,
            "success": self.stats.success,
            "failed": self.stats.failed,
            "current_rate": round(self.stats.current_rate, 2),
            "elapsed_seconds": round(time.time() - self.stats.start_time, 1) if self.stats.start_time else 0,
            "eta_seconds": round(self.stats.eta_seconds, 0),
            "current_delay": round(self.stats.current_delay, 2),
            "adaptive_multiplier": round(self.stats.adaptive_multiplier, 1),
            "buildings_count": self.stats.buildings_count,
            "houses_count": self.stats.houses_count,
            "recent_errors": list(self.stats.recent_errors[-20:]),
        }
        return web.json_response(data)
