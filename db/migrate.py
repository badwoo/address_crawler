"""数据库迁移脚本：旧 schema -> v2 schema
用法: python db/migrate.py
回滚: python db/migrate.py --rollback
"""
import argparse
import asyncio
import logging
import os
import sys

# 将项目根加入 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_config() -> dict:
    cfg = {"host": "localhost", "port": 5432, "user": "postgres", "password": "postgres", "database": "sz_address"}
    for key in cfg:
        env_key = f"ADDR_CRAWLER_{key.upper()}"
        if env_key in os.environ:
            val = os.environ[env_key]
            cfg[key] = int(val) if key == "port" else val
    try:
        import yaml
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.yaml")
        if os.path.exists(config_path):
            with open(config_path, encoding="utf-8") as f:
                yaml_cfg = yaml.safe_load(f) or {}
            db = yaml_cfg.get("database", {})
            cfg.update({k: v for k, v in db.items() if v is not None})
    except ImportError:
        pass
    return cfg


BD_TRACKING_COLUMNS = [
    ("crawl_status", "VARCHAR(20) DEFAULT 'pending'"),
    ("error_msg", "TEXT"),
    ("retry_count", "INTEGER DEFAULT 0"),
    ("updated_at", "TIMESTAMP DEFAULT NOW()"),
]

BD_INDEX = "CREATE INDEX IF NOT EXISTS idx_bd_status ON bd(crawl_status);"


async def table_exists(conn: asyncpg.Connection, name: str) -> bool:
    return await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)", name
    )


async def create_bd_table(conn: asyncpg.Connection):
    """创建 bd 配置表。如果 bd_csv 存在则导入数据。"""
    bd_exists = await table_exists(conn, "bd")
    bd_csv_exists = await table_exists(conn, "bd_csv")

    if bd_exists:
        logger.info("  bd 表已存在，跳过创建")
        return

    if bd_csv_exists:
        logger.info("  从 bd_csv 创建 bd 配置表...")
        await conn.execute("""
            CREATE TABLE bd AS
            SELECT DISTINCT building_code
            FROM bd_csv
            WHERE building_code IS NOT NULL AND building_code != ''
        """)
        await conn.execute("ALTER TABLE bd ADD PRIMARY KEY (building_code)")
        logger.info("  bd 表创建完成（从 bd_csv 导入）")
    else:
        logger.info("  创建空 bd 表...")
        await conn.execute("CREATE TABLE bd (building_code VARCHAR(50) PRIMARY KEY)")
        logger.info("  空 bd 表创建完成（未找到 bd_csv 数据源）")

    # 增加跟踪列
    for col_name, col_def in BD_TRACKING_COLUMNS:
        await conn.execute(f"ALTER TABLE bd ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
    await conn.execute(BD_INDEX)


async def rename_old_table(conn: asyncpg.Connection, table: str):
    """安全重命名旧表（先移除外键约束）"""
    old_name = f"{table}_old"
    exists = await table_exists(conn, table)
    old_exists = await table_exists(conn, old_name)

    if not exists:
        logger.info(f"  表 {table} 不存在，跳过")
        return
    if old_exists:
        logger.info(f"  旧表 {old_name} 已存在，跳过重命名 {table}")
        return

    # 移除外键约束（防止 houses 引用 buildings 导致重命名/删除问题）
    if table == "houses":
        fks = await conn.fetch("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'houses' AND constraint_type = 'FOREIGN KEY'
        """)
        for fk in fks:
            await conn.execute(f"ALTER TABLE houses DROP CONSTRAINT IF EXISTS {fk['constraint_name']}")
            logger.info(f"  移除外键: {fk['constraint_name']}")

    await conn.execute(f"ALTER TABLE {table} RENAME TO {old_name}")
    logger.info(f"  {table} -> {old_name}")


async def create_new_tables(conn: asyncpg.Connection):
    """从 schema_v2.sql 创建新表"""
    schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema_v2.sql")
    with open(schema_path, encoding="utf-8") as f:
        ddl = f.read()
    for statement in ddl.split(";"):
        stmt = statement.strip()
        if stmt and ("CREATE TABLE" in stmt or "CREATE INDEX" in stmt):
            await conn.execute(stmt)
    logger.info("  新表创建完成")


async def migrate(conn: asyncpg.Connection):
    """执行 v2 迁移"""
    logger.info("=== 开始迁移 ===")

    # 1. 创建/更新 bd 配置表
    logger.info("步骤 1/4: 创建 bd 配置表...")
    await create_bd_table(conn)

    # 2. 重命名旧表
    logger.info("步骤 2/4: 重命名旧表...")
    await rename_old_table(conn, "buildings")
    await rename_old_table(conn, "houses")

    # 3. 创建新表
    logger.info("步骤 3/4: 创建新表...")
    await create_new_tables(conn)

    # 4. 验证
    logger.info("步骤 4/4: 验证...")
    for table in ["bd", "buildings", "houses"]:
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        logger.info(f"  {table}: {count} 条记录")

    logger.info("=== 迁移完成 ===")
    logger.info("旧表保留为 buildings_old 和 houses_old，确认无误后可手动删除。")


async def rollback(conn: asyncpg.Connection):
    """回滚迁移"""
    logger.info("=== 开始回滚 ===")
    for table in ["buildings", "houses"]:
        old_name = f"{table}_old"
        old_exists = await table_exists(conn, old_name)
        if old_exists:
            await conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            await conn.execute(f"ALTER TABLE {old_name} RENAME TO {table}")
            logger.info(f"  恢复 {old_name} -> {table}")
        else:
            logger.info(f"  旧表 {old_name} 不存在，跳过")
    logger.info("=== 回滚完成 ===")


async def main():
    parser = argparse.ArgumentParser(description="数据库迁移 v1 -> v2")
    parser.add_argument("--rollback", action="store_true", help="回滚迁移")
    args = parser.parse_args()

    cfg = load_config()
    dsn = f"postgresql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    logger.info(f"数据库: {cfg['host']}:{cfg['port']}/{cfg['database']}")

    conn = await asyncpg.connect(dsn)
    try:
        if args.rollback:
            await rollback(conn)
        else:
            await migrate(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
