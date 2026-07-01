"""主采集流水线 v2 —— 基于 bd 配置表驱动"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .anti_bot import DelayController, AdaptiveRateController, RetryHandler
from .client import ApiClient, HttpStatusError, JsonParseError
from .storage import Storage

logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    """流水线统计（pipeline 与 monitor 间共享）"""
    status: str = "idle"            # idle | running | paused | done
    total_pending: int = 0          # 启动时待处理总数
    processed: int = 0
    success: int = 0
    failed: int = 0
    current_rate: float = 0.0
    elapsed_seconds: float = 0.0
    eta_seconds: float = 0.0
    current_delay: float = 0.0
    adaptive_multiplier: float = 1.0
    recent_errors: list[dict] = field(default_factory=list)  # 最多 50 条
    start_time: float = 0.0
    buildings_count: int = 0
    houses_count: int = 0


class CrawlPipeline:
    """基于 bd 配置表的楼栋数据采集流水线"""

    def __init__(self, client: ApiClient, storage: Storage, config: dict):
        self.client = client
        self.storage = storage
        self.crawl_cfg = config.get("crawler", {})
        self.anti_cfg = config.get("anti_bot", {})

        # 并发控制
        self.concurrency = self.crawl_cfg.get("concurrency", 2)
        self.batch_size = self.crawl_cfg.get("batch_size", 1000)

        # 反爬组件
        self.delay = DelayController(
            min_delay=self.anti_cfg.get("min_delay", 1.0),
            max_delay=self.anti_cfg.get("max_delay", 3.0),
            max_delay_cap=self.anti_cfg.get("max_delay_cap", 60.0),
        )
        self.adaptive = AdaptiveRateController(
            delay=self.delay,
            success_threshold=self.anti_cfg.get("success_threshold", 50),
            rate_decrease_factor=self.anti_cfg.get("rate_decrease_factor", 2.0),
            rate_increase_factor=self.anti_cfg.get("rate_increase_factor", 0.9),
            enabled=self.anti_cfg.get("adaptive_rate", True),
        )
        self.retry = RetryHandler(
            max_retries=self.anti_cfg.get("max_retries", 3),
            base_delay=self.anti_cfg.get("retry_base_delay", 5.0),
            backoff=self.anti_cfg.get("retry_backoff", 2.0),
        )

        # 状态
        self.stats = PipelineStats()
        self._shutdown = False
        self._rate_window: list[float] = []  # 最近 60 秒的完成时间戳

    def request_shutdown(self):
        self._shutdown = True

    async def run(self):
        """主运行循环"""
        self.stats.status = "running"
        self.stats.start_time = time.time()
        logger.info(f"=== 采集流水线启动 === 并发: {self.concurrency}, 批次: {self.batch_size}")

        # 获取待处理总数
        self.stats.total_pending = await self.storage.get_total_pending(
            max_retries=self.anti_cfg.get("max_retries", 3)
        )
        logger.info(f"待处理: {self.stats.total_pending} 个 building_code")

        if self.stats.total_pending == 0:
            self.stats.status = "done"
            logger.info("无待处理数据，结束")
            return

        # 批次循环
        while not self._shutdown:
            # 从数据库取一批待处理 code
            codes = await self.storage.get_pending_codes(
                batch_size=self.batch_size,
                max_retries=self.anti_cfg.get("max_retries", 3),
            )
            if not codes:
                break

            logger.info(f"获取批次: {len(codes)} 个 code")

            # 放入队列
            queue: asyncio.Queue = asyncio.Queue()
            for code in codes:
                await queue.put(code)

            # 启动 worker
            workers = [
                asyncio.create_task(self._worker(i, queue))
                for i in range(self.concurrency)
            ]

            # 等待所有 worker 完成
            await asyncio.gather(*workers)

            # 更新数据库统计
            try:
                db_stats = await self.storage.get_stats()
                self.stats.buildings_count = db_stats.get("buildings_count", 0)
                self.stats.houses_count = db_stats.get("houses_count", 0)
            except Exception:
                pass

            if self._shutdown:
                break

        self.stats.status = "done"
        elapsed = time.time() - self.stats.start_time
        logger.info(
            f"=== 采集完成 ===\n"
            f"处理: {self.stats.processed} | 成功: {self.stats.success} | 失败: {self.stats.failed} | "
            f"耗时: {elapsed/60:.1f} 分钟"
        )

    async def _worker(self, worker_id: int, queue: asyncio.Queue):
        """单个 worker 协程"""
        while not self._shutdown:
            try:
                code = queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            try:
                await self._process_one(code)
            except Exception as e:
                logger.error(f"Worker-{worker_id} 处理 {code} 异常: {e}")
            finally:
                queue.task_done()

    async def _process_one(self, code: str):
        """处理单个 building_code"""
        # 随机延迟
        await self.delay.wait()

        # 带重试的请求
        result, error = await self.retry.execute(
            self.client.fetch_building_detail,
            code,
            is_block_error=lambda e: _is_block_error(e),
        )

        if result is None:
            # 所有重试均失败
            err_msg = str(error) if error else "未知错误"
            await self.storage.mark_code_failed(code, err_msg)
            self.stats.processed += 1
            self.stats.failed += 1
            self.adaptive.report_failure()
            self._record_error(code, err_msg)
            logger.error(f"失败 {code}: {err_msg[:80]}")
            return

        # 检查 API 层响应
        data = result
        if not data.get("success"):
            msg = data.get("message", "API 返回 success=false")[:200]
            await self.storage.mark_code_failed(code, msg)
            self.stats.processed += 1
            self.stats.failed += 1
            self.adaptive.report_failure()
            self._record_error(code, msg)
            logger.warning(f"API 失败 {code}: {msg[:80]}")
            return

        # 成功：入库
        api_result = data.get("result")
        if not api_result:
            # === Fallback: 尝试 customSearch ===
            fallback_ok = await self._fallback_custom_search(code)
            if fallback_ok:
                await self.storage.mark_code_success(code)
                self.stats.processed += 1
                self.stats.success += 1
                self.adaptive.report_success()
                self._record_rate()
                self._maybe_log_progress(house_count=0)
                return

            await self.storage.mark_code_failed(code, "API 返回空 result, customSearch 也未匹配")
            self.stats.processed += 1
            self.stats.failed += 1
            self.adaptive.report_failure()
            self._record_error(code, "API 返回空 result, customSearch 也未匹配")
            return

        try:
            house_count = await self.storage.upsert_building(api_result)
        except Exception as e:
            await self.storage.mark_code_failed(code, f"入库失败: {e}")
            self.stats.processed += 1
            self.stats.failed += 1
            self.adaptive.report_failure()
            self._record_error(code, f"入库: {e}")
            return

        await self.storage.mark_code_success(code)
        self.stats.processed += 1
        self.stats.success += 1
        self.adaptive.report_success()
        self._record_rate()
        self._maybe_log_progress(house_count=house_count)

    def _record_rate(self):
        """记录速率采样"""
        self._rate_window.append(time.time())
        # 只保留最近 60 秒
        cutoff = time.time() - 60
        self._rate_window = [t for t in self._rate_window if t > cutoff]

    def _record_error(self, code: str, error: str):
        """记录错误（最近 50 条）"""
        self.stats.recent_errors.append({
            "time": time.strftime("%H:%M:%S"),
            "building_code": code,
            "error": str(error)[:200],
        })
        if len(self.stats.recent_errors) > 50:
            self.stats.recent_errors = self.stats.recent_errors[-50:]

    def _maybe_log_progress(self, house_count: int = 0):
        """每 100 条成功时输出进度日志"""
        if self.stats.success % 100 == 0:
            elapsed = time.time() - self.stats.start_time
            rate = self.stats.processed / elapsed if elapsed > 0 else 0
            self.stats.current_rate = rate
            self.stats.elapsed_seconds = elapsed
            if self.stats.processed > 0:
                self.stats.eta_seconds = (
                    (self.stats.total_pending - self.stats.processed) / rate
                    if rate > 0 else 0
                )
            self.stats.current_delay = self.delay.current_delay()
            self.stats.adaptive_multiplier = self.delay.adaptive_multiplier
            logger.info(
                f"进度: {self.stats.processed}/{self.stats.total_pending} | "
                f"成功: {self.stats.success} 失败: {self.stats.failed} | "
                f"房屋: {house_count} | {rate:.1f} req/s | "
                f"延迟倍率: {self.delay.adaptive_multiplier:.1f}x"
            )

    async def _fallback_custom_search(self, code: str) -> bool:
        """customSearch fallback: 当 buildingDetail 返回空 result 时尝试。
        返回 True 表示匹配并写入成功，False 表示失败。
        """
        logger.info(f"buildingDetail 返回空 result，尝试 customSearch fallback: {code}")

        try:
            data = await self.client.fetch_custom_search(code)
        except Exception as e:
            logger.warning(f"customSearch 请求失败 {code}: {e}")
            return False

        # customSearch 使用 status=0 表示成功（而非 success=true）
        if data.get("status") != 0:
            logger.warning(f"customSearch API status={data.get('status')}: {code}")
            return False

        results = data.get("result", []) or []
        if not results:
            logger.warning(f"customSearch 返回空 result: {code}")
            return False

        # 匹配：用 uid 字段精确匹配目标 building_code
        matched = None
        for item in results:
            if item.get("uid") == code:
                matched = item
                break

        if matched is None:
            logger.warning(
                f"customSearch 返回 {len(results)} 条结果但无匹配 uid={code}。"
                f"返回的 uid: {[r.get('uid') for r in results]}"
            )
            return False

        try:
            building_id = await self.storage.upsert_building_from_custom_search(matched)
            if building_id:
                logger.info(f"customSearch fallback 成功: {code} -> building_id={building_id}")
                return True
            else:
                logger.warning(f"customSearch 结果缺少 id(uid) 字段: {code}")
                return False
        except Exception as e:
            logger.error(f"customSearch 入库失败 {code}: {e}")
            return False

    def get_rate(self) -> float:
        """计算当前速率（req/s）"""
        if not self._rate_window:
            return 0.0
        window_span = time.time() - self._rate_window[0]
        if window_span <= 0:
            return 0.0
        return len(self._rate_window) / window_span


def _is_block_error(e: Exception) -> bool:
    """判断是否为限流/封禁错误"""
    # 明确的 HTTP 状态码异常
    if isinstance(e, HttpStatusError):
        return e.status_code in (403, 429, 503, 502, 504)

    msg = str(e).lower()
    # HTTP 403 / 429
    if "403" in msg or "429" in msg:
        return True
    # 常见限流关键词
    for kw in ["blocked", "rate limit", "too many", "forbidden", "captcha", "challenge"]:
        if kw in msg:
            return True
    # JSON 解析失败且响应为空/非 JSON，往往是 WAF 拦截返回的 HTML/空体
    if isinstance(e, JsonParseError) and ("<empty body>" in msg or "unexpected character" in msg):
        return True
    return False
