"""多级关键词枚举器——生成搜索关键词，收集 buildingId"""
import asyncio
import json
import logging
import os
from .client import ApiClient

logger = logging.getLogger(__name__)

DISTRICTS = ["南山区", "福田区", "罗湖区", "宝安区", "龙岗区", "龙华区", "光明区", "坪山区", "盐田区", "大鹏新区"]
SUFFIX_CHARS = ["路", "街", "巷", "村", "园", "苑", "大厦", "花园", "公寓"]

CHECKPOINT_FILE = "enumerator_checkpoint.json"


class BuildingEnumerator:

    def __init__(self, client: ApiClient):
        self.client = client
        self.seen_uids: set = set()
        self.community_counts: dict[str, int] = {}  # 社区名 -> 返回结果数

    def _save_checkpoint(self, stage: str, idx: int = 0):
        try:
            with open(CHECKPOINT_FILE, "w") as f:
                json.dump({
                    "stage": stage,
                    "index": idx,
                    "uid_count": len(self.seen_uids),
                    "uids": list(self.seen_uids)
                }, f)
        except Exception:
            pass

    def _load_checkpoint(self) -> dict | None:
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE) as f:
                    data = json.load(f)
                    self.seen_uids = set(data.get("uids", []))
                    return data
            except Exception:
                pass
        return None

    async def collect_communities(self) -> list[str]:
        communities = []
        page = 1
        while True:
            data = await self.client.get_grid_page(page=page, page_size=100)
            if not data.get("success"):
                break
            result = data.get("result", {})
            items = result.get("list", [])
            if not items:
                break
            for item in items:
                name = item.get("orgname", "")
                if name:
                    communities.append(name)
            total = int(result.get("total", 0))
            if len(communities) >= total:
                break
            page += 1
        return communities

    async def search_by_keyword(self, keyword: str) -> list[str]:
        try:
            data = await self.client.search_buildings(keyword)
            if data.get("status") != 0:
                return []
            results = data.get("result", [])
            uids = []
            for item in results:
                uid = item.get("uid", "")
                if uid and uid not in self.seen_uids:
                    self.seen_uids.add(uid)
                    uids.append(uid)
            return uids
        except Exception as e:
            logger.error(f"搜索 '{keyword}' 失败: {e}")
            return []

    async def enumerate(self) -> list[str]:
        cp = self._load_checkpoint()
        if cp:
            start_stage = cp.get("stage", "L1")
            start_idx = cp.get("index", 0)
        else:
            start_stage = "L1"
            start_idx = 0
        logger.info(f"=== 枚举开始 (从 {start_stage}:{start_idx} 继续) ===")

        # L1: 获取所有社区名
        communities = []
        if start_stage <= "L1":
            logger.info("L1: 获取社区列表...")
            communities = await self.collect_communities()
            self._save_checkpoint("L2", 0)
            logger.info(f"L1 完成: {len(communities)} 个社区")
        else:
            communities = await self.collect_communities()

        # L2: 按社区名搜索
        if start_stage <= "L2":
            logger.info(f"L2: 按社区名搜索 (从 {start_idx})...")
            for i in range(start_idx, len(communities)):
                name = communities[i]
                uids = await self.search_by_keyword(name)
                self.community_counts[name] = len(uids)
                if (i + 1) % 100 == 0:
                    logger.info(f"  L2: {i+1}/{len(communities)}, UID: {len(self.seen_uids)}")
                    self._save_checkpoint("L2", i + 1)

        # L3: 按区名搜索
        if start_stage <= "L3":
            logger.info("L3: 按区名搜索...")
            for i, d in enumerate(DISTRICTS):
                if start_stage == "L3" and i < start_idx:
                    continue
                await self.search_by_keyword(d)
            self._save_checkpoint("L4", 0)
        else:
            logger.info("L3: 跳过")

        # L4: 特征字搜索
        if start_stage <= "L4":
            logger.info("L4: 按特征字搜索...")
            for i, ch in enumerate(SUFFIX_CHARS):
                if start_stage == "L4" and i < start_idx:
                    continue
                await self.search_by_keyword(ch)
            self._save_checkpoint("L5", 0)
        else:
            logger.info("L4: 跳过")

        # L5: 对满结果(10条)的社区做后缀分解（限 top 100）
        if start_stage <= "L5":
            full = sorted(self.community_counts.items(), key=lambda x: -x[1])
            full_communities = [c for c, n in full if n >= 10][:100]
            logger.info(f"L5: 后缀分解 ({len(full_communities)} 个满结果社区)...")
            count = 0
            for i, name in enumerate(full_communities):
                for n in range(1, 11):
                    await self.search_by_keyword(f"{name} {n}栋")
                    await self.search_by_keyword(f"{name} {n}号")
                    count += 2
                if count >= 100:
                    logger.info(f"  L5: {count} 次搜索, UID: {len(self.seen_uids)}")
                    self._save_checkpoint("L5", i + 1)
                    count = 0

        # 清理检查点
        try:
            os.remove(CHECKPOINT_FILE)
        except Exception:
            pass

        logger.info(f"=== 枚举完成: {len(self.seen_uids)} 个唯一楼栋 ===")
        return list(self.seen_uids)
