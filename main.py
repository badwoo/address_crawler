"""深圳统一地址查询 - 楼栋房屋数据抓取 v2
数据来源: https://spatydz.sz.gov.cn
"""
import asyncio
import logging
import os
import signal
import sys

from crawler.anti_bot import DelayController
from crawler.client import ApiClient
from crawler.monitor import MonitorServer
from crawler.pipeline import CrawlPipeline
from crawler.storage import Storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_config() -> dict:
    """从 yaml 和环境变量加载配置"""
    # 默认值
    cfg = {
        "database": {
            "host": "localhost", "port": 5432,
            "user": "postgres", "password": "postgres",
            "database": "sz_address",
        },
        "crawler": {"concurrency": 2, "batch_size": 1000, "resume": True},
        "anti_bot": {
            "min_delay": 1.0, "max_delay": 3.0,
            "impersonate": ["chrome", "firefox", "safari", "edge"],
            "stealthy_headers": True,
            "extra_headers": {
                "Referer": "https://spatydz.sz.gov.cn/addrdatapc/standard/search",
                "Origin": "https://spatydz.sz.gov.cn",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            },
            "proxy": None,
            "max_retries": 3, "retry_base_delay": 5.0, "retry_backoff": 2.0,
            "adaptive_rate": True, "max_delay_cap": 60.0,
            "success_threshold": 50, "rate_decrease_factor": 2.0, "rate_increase_factor": 0.9,
        },
        "monitor": {"enabled": True, "host": "0.0.0.0", "port": 8888, "refresh_interval": 3},
    }

    # 环境变量覆盖 (ADDR_CRAWLER_ 前缀)
    env_map = {
        "ADDR_CRAWLER_HOST": ("database", "host"),
        "ADDR_CRAWLER_PORT": ("database", "port", int),
        "ADDR_CRAWLER_USER": ("database", "user"),
        "ADDR_CRAWLER_PASSWORD": ("database", "password"),
        "ADDR_CRAWLER_DATABASE": ("database", "database"),
        "ADDR_CRAWLER_CONCURRENCY": ("crawler", "concurrency", int),
        "ADDR_CRAWLER_MIN_DELAY": ("anti_bot", "min_delay", float),
        "ADDR_CRAWLER_MAX_DELAY": ("anti_bot", "max_delay", float),
        "ADDR_CRAWLER_PROXY": ("anti_bot", "proxy"),
        "ADDR_CRAWLER_MONITOR_PORT": ("monitor", "port", int),
    }
    for env_key, path in env_map.items():
        if env_key in os.environ:
            val = os.environ[env_key]
            section, key = path[0], path[1]
            convert = path[2] if len(path) > 2 else str
            cfg[section][key] = convert(val)

    # yaml 文件覆盖
    try:
        import yaml
        if os.path.exists("config.yaml"):
            with open("config.yaml", encoding="utf-8") as f:
                yaml_cfg = yaml.safe_load(f) or {}
            for section in cfg:
                if section in yaml_cfg:
                    cfg[section].update({k: v for k, v in yaml_cfg[section].items() if v is not None})
    except ImportError:
        pass

    return cfg


def build_dsn(db: dict) -> str:
    return f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"


async def main():
    cfg = load_config()

    dsn = build_dsn(cfg["database"])
    logger.info(f"数据库: {cfg['database']['host']}:{cfg['database']['port']}/{cfg['database']['database']}")
    logger.info(f"并发: {cfg['crawler']['concurrency']} | 延迟: {cfg['anti_bot']['min_delay']}-{cfg['anti_bot']['max_delay']}s")

    # 初始化组件
    storage = Storage(dsn)
    await storage.connect()

    client = ApiClient(cfg["anti_bot"])
    pipeline = CrawlPipeline(client, storage, cfg)

    # 启动监控
    monitor = None
    if cfg["monitor"].get("enabled", True):
        monitor = MonitorServer(pipeline.stats, cfg["monitor"])
        await monitor.start()

    # 信号处理
    shutdown_event = asyncio.Event()

    def on_shutdown():
        logger.info("收到终止信号，等待进行中的任务完成...")
        pipeline.request_shutdown()
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, on_shutdown)
        loop.add_signal_handler(signal.SIGTERM, on_shutdown)
    except NotImplementedError:
        # Windows 不支持 add_signal_handler
        signal.signal(signal.SIGINT, lambda s, f: on_shutdown())

    # 运行
    pipeline_task = asyncio.create_task(pipeline.run())

    # 等待完成或收到关闭信号
    done, _ = await asyncio.wait(
        [pipeline_task, asyncio.create_task(shutdown_event.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )

    if shutdown_event.is_set():
        await pipeline_task

    # 清理
    if monitor:
        await monitor.stop()
    await client.close()
    await storage.close()
    logger.info("程序已退出")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
