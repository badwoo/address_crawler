"""反爬策略模块

分层防御:
  L1: TLS 指纹随机化 (Scrapling impersonate 列表)
  L2: 隐身 Headers (stealthy_headers=True)
  L3: 随机延迟 + 抖动
  L4: 自适应速率 (遇 403/429 减速, 持续成功逐步恢复)
  L5: 指数退避重试
  L6: 浏览器升级 (StealthyFetcher)
  L7: 可选代理
"""
import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class DelayController:
    """随机延迟控制器，支持自适应倍率"""

    min_delay: float = 1.0
    max_delay: float = 3.0
    max_delay_cap: float = 60.0
    adaptive_multiplier: float = 1.0

    def current_delay(self) -> float:
        """当前有效延迟（用于监控展示）"""
        return random.uniform(self.min_delay, self.max_delay) * self.adaptive_multiplier

    async def wait(self):
        """随机等待 [min_delay, max_delay] * multiplier 秒，上限 max_delay_cap"""
        raw = random.uniform(self.min_delay, self.max_delay)
        delay = min(raw * self.adaptive_multiplier, self.max_delay_cap)
        if delay > 0:
            await asyncio.sleep(delay)

    def slowdown(self, factor: float = 2.0):
        """减速：倍率 *= factor"""
        old = self.adaptive_multiplier
        self.adaptive_multiplier = min(self.adaptive_multiplier * factor, self.max_delay_cap / self.min_delay)
        if self.adaptive_multiplier != old:
            logger.warning(f"触发减速: 延迟倍率 {old:.1f}x -> {self.adaptive_multiplier:.1f}x")

    def speedup(self, factor: float = 0.9):
        """加速：倍率 *= factor（不低于 1.0）"""
        old = self.adaptive_multiplier
        self.adaptive_multiplier = max(self.adaptive_multiplier * factor, 1.0)
        if self.adaptive_multiplier != old:
            logger.info(f"逐步恢复: 延迟倍率 {old:.1f}x -> {self.adaptive_multiplier:.1f}x")


@dataclass
class AdaptiveRateController:
    """自适应速率：跟踪连续成功/失败，自动调速"""

    delay: DelayController
    success_threshold: int = 50
    rate_decrease_factor: float = 2.0
    rate_increase_factor: float = 0.9
    consecutive_successes: int = 0
    enabled: bool = True

    def report_success(self):
        """报告一次成功"""
        if not self.enabled:
            return
        self.consecutive_successes += 1
        if self.consecutive_successes >= self.success_threshold:
            self.delay.speedup(self.rate_increase_factor)
            self.consecutive_successes = 0

    def report_block(self):
        """报告被限流（403/429）"""
        if not self.enabled:
            return
        self.delay.slowdown(self.rate_decrease_factor)
        self.consecutive_successes = 0

    def report_failure(self):
        """报告失败（非限流类错误）"""
        self.consecutive_successes = 0

    @property
    def is_slowed(self) -> bool:
        return self.delay.adaptive_multiplier > 1.0


@dataclass
class RetryHandler:
    """指数退避重试"""

    max_retries: int = 3
    base_delay: float = 5.0
    backoff: float = 2.0
    jitter: float = 0.25  # ±25% 抖动

    def retry_delay(self, attempt: int) -> float:
        """计算第 attempt 次重试的等待时间（秒）"""
        delay = self.base_delay * (self.backoff ** attempt)
        jitter_amount = delay * self.jitter
        return delay + random.uniform(-jitter_amount, jitter_amount)

    async def execute(self, fn, *args, is_block_error=None, **kwargs):
        """执行 fn(*args, **kwargs)，失败时自动重试。

        is_block_error(exception) -> bool: 判断是否为限流错误（403/429等），
        用于上报到 AdaptiveRateController。
        返回 (result, None) 或 (None, last_error)。
        """
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                result = await fn(*args, **kwargs)
                return result, None
            except Exception as e:
                last_error = e
                if is_block_error and is_block_error(e):
                    logger.warning(f"检测到限流 (attempt {attempt+1}/{self.max_retries+1}): {e}")
                else:
                    logger.warning(f"请求失败 (attempt {attempt+1}/{self.max_retries+1}): {e}")

                if attempt < self.max_retries:
                    delay = self.retry_delay(attempt)
                    logger.info(f"  等待 {delay:.1f}s 后重试...")
                    await asyncio.sleep(delay)

        return None, last_error
