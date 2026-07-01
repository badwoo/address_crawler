"""API 客户端——基于 Scrapling Fetcher"""
import asyncio
import logging
import random
from typing import Optional

from scrapling.fetchers import Fetcher

logger = logging.getLogger(__name__)

BASE_URL = "https://spatydz.sz.gov.cn"
BASE_API = "addrdatapc"
AK = "d129375bf07f409a8e5d2ae232712b2a"
REGION = 440300

# 模拟从站内搜索页发起的请求头
DEFAULT_EXTRA_HEADERS = {
    "Referer": f"{BASE_URL}/{BASE_API}/standard/search",
    "Origin": BASE_URL,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


class HttpStatusError(Exception):
    """HTTP 状态码异常（如 403/429/503）"""

    def __init__(self, status_code: int, url: str, body_preview: str = ""):
        self.status_code = status_code
        self.url = url
        self.body_preview = body_preview
        super().__init__(f"HTTP {status_code} from {url}: {body_preview[:120]}")


class JsonParseError(Exception):
    """响应体 JSON 解析失败"""

    def __init__(self, message: str, body_preview: str = ""):
        self.body_preview = body_preview
        super().__init__(f"{message} | body: {body_preview[:120]}")


def _parse_response(resp, url: str, label: str = "") -> dict:
    """统一解析响应：检查状态码、解析 JSON、记录异常快照。"""
    status = getattr(resp, "status_code", None) or getattr(resp, "status", 0)
    text = ""
    try:
        text = resp.text
    except Exception:
        pass

    # 把 4xx/5xx 显式抛出，避免下游拿到空 JSON 时莫名其妙
    if status and status >= 400:
        preview = text[:200].strip() or "<empty body>"
        logger.warning(f"{label} 收到 HTTP {status}: {preview[:100]}")
        raise HttpStatusError(status, url, preview)

    try:
        return resp.json()
    except Exception as e:
        preview = text[:200].strip() or "<empty body>"
        logger.warning(f"{label} JSON 解析失败: {preview[:100]}")
        raise JsonParseError(str(e), preview) from e


class ApiClient:
    """深圳统一地址查询 API 客户端"""

    def __init__(self, config: dict):
        """
        config: anti_bot 配置字典
          - impersonate: list[str]  浏览器指纹列表，每次请求随机选
          - stealthy_headers: bool
          - extra_headers: dict | None  额外请求头（合并到 stealthy_headers 中）
          - proxy: str | None       代理 URL
        """
        self.impersonate = config.get("impersonate", ["chrome"])
        if isinstance(self.impersonate, str):
            self.impersonate = [self.impersonate]
        self.stealthy_headers = config.get("stealthy_headers", True)
        self.proxy = config.get("proxy")

        # 用户可覆盖默认请求头；传入 None 表示使用默认
        user_extra = config.get("extra_headers")
        self.headers: dict = {**DEFAULT_EXTRA_HEADERS, **(user_extra or {})}

    def _random_impersonate(self) -> str:
        """从列表中随机选取浏览器指纹"""
        return random.choice(self.impersonate)

    def _request_kwargs(self) -> dict:
        """构造每次请求的公共参数"""
        return {
            "impersonate": self._random_impersonate(),
            "stealthy_headers": self.stealthy_headers,
            "proxy": self.proxy,
            "headers": self.headers,
        }

    async def fetch_building_detail(self, building_code: str) -> dict:
        """获取楼栋详情（包含房屋数据）"""
        params = {
            "buildingId": building_code,
            "ak": AK,
            "t": int(asyncio.get_event_loop().time() * 1000),
        }
        url = f"{BASE_URL}/{BASE_API}/standard/search/buildingDetail"

        # Fetcher.get 是同步的，在线程池中运行
        resp = await asyncio.to_thread(
            Fetcher.get,
            url,
            params=params,
            **self._request_kwargs(),
        )
        return _parse_response(resp, url, label=f"buildingDetail[{building_code}]")

    async def fetch_custom_search(self, query: str) -> dict:
        """搜索楼栋（customSearch 接口），用于 buildingDetail 空结果的 fallback"""
        params = {
            "query": query,
            "region": REGION,
            "page": 1,
            "pageSize": 10,
            "ak": AK,
            "t": int(asyncio.get_event_loop().time() * 1000),
        }
        url = f"{BASE_URL}/{BASE_API}/standard/search/customSearch"

        resp = await asyncio.to_thread(
            Fetcher.get,
            url,
            params=params,
            **self._request_kwargs(),
        )
        return _parse_response(resp, url, label=f"customSearch[{query}]")

    async def close(self):
        """清理资源（当前使用无状态 Fetcher，无需特殊清理）"""
        pass
