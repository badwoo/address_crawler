"""PostgreSQL 存储层 v2"""
import logging
from datetime import datetime
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

# buildingDetail.outerAddressStandard 的所有字段（camelCase，与 API 一致）
BUILDING_COLS = [
    "id", "sortNum", "parentId",
    "province", "provinceCode", "city", "cityCode",
    "county", "countyCode", "town", "townCode",
    "community", "communityCode", "adcode",
    "road", "roadNo", "type", "aoi", "subAoi",
    "building", "unit", "floor", "room",
    "aliasList", "address", "aoiId", "buildingId",
    "x", "y", "geom", "bgId",
    "createBy", "createTime", "modifyBy", "modifyTime",
    "source", "md5Id", "md5ParentId",
    "addrMark", "businessId", "businessAddress", "businessAddrSrc",
    "extFields", "updatetime", "rowColumns",
    "name", "housephoto", "qianhaiFlag",
]

# floorList[].room[] 的所有字段
HOUSE_COLS = [
    "id", "parentId", "building", "room", "address", "name",
    "buildingId", "x", "y",
]

BD_COLS = ["building_code", "crawl_status", "error_msg", "retry_count", "updated_at"]


class Storage:
    """PostgreSQL 数据存储"""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    # ---- 连接管理 ----

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)
        await self.ensure_schema()
        logger.info("数据库已连接，表已就绪")

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def ensure_schema(self):
        """幂等建表（与 db/schema_v2.sql 一致）"""
        async with self.pool.acquire() as conn:
            # bd 跟踪列（表由用户创建，这里补列）
            for col, col_def in [
                ("crawl_status", "VARCHAR(20) DEFAULT 'pending'"),
                ("error_msg", "TEXT"),
                ("retry_count", "INTEGER DEFAULT 0"),
                ("updated_at", "TIMESTAMP DEFAULT NOW()"),
            ]:
                await conn.execute(f"ALTER TABLE bd ADD COLUMN IF NOT EXISTS {col} {col_def}")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_bd_status ON bd(crawl_status)")

            # buildings
            building_col_defs = ",\n".join(
                f"    {c} {_col_type(c)}" for c in BUILDING_COLS
            )
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS buildings (
                    {building_col_defs},
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (id)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_buildings_buildingId ON buildings(buildingId)")

            # houses
            house_col_defs = ",\n".join(
                f"    {c} {_col_type(c)}" for c in HOUSE_COLS
            )
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS houses (
                    {house_col_defs},
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (id)
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_houses_buildingId ON houses(buildingId)")

    # ---- 数据提取 ----

    @staticmethod
    def _extract_building(result: dict) -> dict:
        """从 buildingDetail 响应提取楼栋字段。
        outerAddressStandard 的所有字段原样存储，x/y 来自顶层 result。
        sortNum 转为整数，x/y 转为浮点数。
        """
        outer = result.get("outerAddressStandard", {}) or {}
        row = {}
        for col in BUILDING_COLS:
            if col in ("x", "y"):
                val = result.get(col)
                try:
                    row[col] = float(val) if val is not None else None
                except (ValueError, TypeError):
                    row[col] = None
            elif col == "sortNum":
                val = outer.get(col)
                try:
                    row[col] = int(val) if val is not None else None
                except (ValueError, TypeError):
                    row[col] = None
            else:
                val = outer.get(col)
                # aliasList/extFields 可能是 list/dict，序列化为 JSON 字符串
                if isinstance(val, (list, dict)):
                    import json
                    val = json.dumps(val, ensure_ascii=False)
                row[col] = val
        return row

    # ---- customSearch 数据标准化 ----

    @staticmethod
    def _normalize_custom_search_item(item: dict) -> dict:
        """将 customSearch 返回的楼栋对象标准化为 buildingDetail 兼容格式。

        customSearch 返回扁平结构（无 outerAddressStandard 嵌套），字段名也不同：
        - uid → id / buildingId
        - name → name / building
        - district → county
        - location.lng/lat → x/y
        - type_name → type
        """
        loc = item.get("location") or {}
        outer = {
            "id": item.get("uid"),
            "buildingId": item.get("uid"),
            "name": item.get("name"),
            "building": item.get("name"),
            "address": item.get("address"),
            "province": item.get("province"),
            "city": item.get("city"),
            "county": item.get("district"),
            "town": item.get("town"),
            "adcode": item.get("adcode"),
            "type": item.get("type_name") or item.get("exttype"),
            "parentId": item.get("parent_id"),
            "businessId": item.get("business_id"),
            "businessAddress": (item.get("std_addr_address") or ""),
            "source": item.get("datasource"),
            "aoiId": item.get("std_addr_id"),
        }
        # 清理 None 值，避免覆盖已有数据的字段为 None
        outer = {k: v for k, v in outer.items() if v is not None}
        return {
            "outerAddressStandard": outer,
            "x": loc.get("lng"),
            "y": loc.get("lat"),
        }

    @staticmethod
    def _extract_houses(result: dict) -> list[dict]:
        """从 buildingDetail 响应提取房屋列表。
        遍历 floorList[].room[]，每个 room 字段原样提取；x/y 转为浮点数。
        """
        houses = []
        for floor in result.get("floorList", []) or []:
            for room in (floor.get("room", []) or []):
                row = {}
                for col in HOUSE_COLS:
                    val = room.get(col)
                    if col in ("x", "y") and val is not None:
                        try:
                            val = float(val)
                        except (ValueError, TypeError):
                            val = None
                    row[col] = val
                houses.append(row)
        return houses

    # ---- 入库 ----

    async def upsert_building(self, result: dict) -> int:
        """插入或更新楼栋及其房屋，返回房屋数"""
        b = self._extract_building(result)
        houses = self._extract_houses(result)
        building_id = b.get("id", "")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Upsert 楼栋
                cols = [c for c in BUILDING_COLS if c in b]
                placeholders = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
                col_names = ", ".join(cols)
                values = [b[c] for c in cols]
                update_set = ", ".join(
                    f"{c}=EXCLUDED.{c}" for c in cols if c != "id"
                )
                await conn.execute(f"""
                    INSERT INTO buildings ({col_names}) VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET {update_set}, updated_at = NOW()
                """, *values)

                # Upsert 房屋（逐条）
                if houses:
                    hcols = [c for c in HOUSE_COLS if c in houses[0]]
                    h_col_names = ", ".join(hcols)
                    h_placeholders = ", ".join(f"${i}" for i in range(1, len(hcols) + 1))
                    h_update = ", ".join(f"{c}=EXCLUDED.{c}" for c in hcols if c != "id")
                    for h in houses:
                        h_vals = [h.get(c) for c in hcols]
                        await conn.execute(f"""
                            INSERT INTO houses ({h_col_names}) VALUES ({h_placeholders})
                            ON CONFLICT (id) DO UPDATE SET {h_update}, updated_at = NOW()
                        """, *h_vals)

        return len(houses)

    async def upsert_building_from_custom_search(self, item: dict) -> str | None:
        """从 customSearch 结果写入楼栋表（不写房屋表）。
        返回写入的 building_id，失败返回 None。
        只写入有实际值的字段，避免用 None 覆盖已有数据。
        """
        normalized = self._normalize_custom_search_item(item)
        b = self._extract_building(normalized)
        building_id = b.get("id", "")

        if not building_id:
            logger.warning("customSearch 结果缺少 id(uid) 字段，无法写入")
            return None

        # 只保留有实际值的字段，避免用 None 覆盖已有完整数据
        non_null_cols = [c for c in BUILDING_COLS if c in b and b[c] is not None]
        placeholders = ", ".join(f"${i}" for i in range(1, len(non_null_cols) + 1))
        col_names = ", ".join(non_null_cols)
        values = [b[c] for c in non_null_cols]
        update_set = ", ".join(
            f"{c}=EXCLUDED.{c}" for c in non_null_cols if c != "id"
        )

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f"""
                    INSERT INTO buildings ({col_names}) VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET {update_set}, updated_at = NOW()
                """, *values)

        logger.info(f"customSearch 写入楼栋: {building_id} (写入 {len(non_null_cols)} 个非空字段)")
        return building_id

    # ---- bd 表操作 ----

    async def get_pending_codes(self, batch_size: int = 1000, max_retries: int = 3) -> list[str]:
        """获取待抓取的 building_code 列表"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT building_code FROM bd
                WHERE crawl_status = 'pending'
                   OR (crawl_status = 'failed' AND retry_count < $2)
                ORDER BY building_code
                LIMIT $1
            """, batch_size, max_retries)
            return [r["building_code"] for r in rows]

    async def mark_code_success(self, code: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE bd SET crawl_status = 'success', error_msg = NULL, updated_at = NOW()
                WHERE building_code = $1
            """, code)

    async def mark_code_failed(self, code: str, error: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE bd SET crawl_status = 'failed', error_msg = $2,
                       retry_count = retry_count + 1, updated_at = NOW()
                WHERE building_code = $1
            """, code, error[:500] if error else "")

    async def get_total_pending(self, max_retries: int = 3) -> int:
        """待处理总数"""
        async with self.pool.acquire() as conn:
            return await conn.fetchval("""
                SELECT COUNT(*) FROM bd
                WHERE crawl_status = 'pending'
                   OR (crawl_status = 'failed' AND retry_count < $1)
            """, max_retries)

    async def get_stats(self) -> dict:
        """获取整体统计"""
        async with self.pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM bd")
            success = await conn.fetchval("SELECT COUNT(*) FROM bd WHERE crawl_status = 'success'")
            failed = await conn.fetchval("SELECT COUNT(*) FROM bd WHERE crawl_status = 'failed'")
            pending = await conn.fetchval("SELECT COUNT(*) FROM bd WHERE crawl_status = 'pending'")
            b_count = await conn.fetchval("SELECT COUNT(*) FROM buildings")
            h_count = await conn.fetchval("SELECT COUNT(*) FROM houses")
        return {
            "total": total or 0,
            "success": success or 0,
            "failed": failed or 0,
            "pending": pending or 0,
            "buildings_count": b_count or 0,
            "houses_count": h_count or 0,
        }

    # ---- 兼容旧接口 ----

    async def get_existing_uids(self) -> set[str]:
        """获取已入库楼栋 ID（旧接口兼容）"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT id FROM buildings")
            return {r["id"] for r in rows}

    async def get_building_count(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM buildings") or 0

    async def get_house_count(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM houses") or 0


def _col_type(col: str) -> str:
    """根据列名推断 PostgreSQL 类型"""
    if col in ("x", "y"):
        return "DOUBLE PRECISION"
    if col in ("sortNum",):
        return "INTEGER"
    if col in ("address", "aliasList", "extFields", "geom", "rowColumns",
               "businessAddress", "housephoto"):
        return "TEXT"
    return "VARCHAR(500)"
