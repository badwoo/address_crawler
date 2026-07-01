-- 深圳统一地址查询 - 楼栋与房屋数据
-- 数据来源：spatydz.sz.gov.cn buildingDetail 接口

CREATE TABLE IF NOT EXISTS buildings (
    uid             VARCHAR(50) PRIMARY KEY,
    name            VARCHAR(200),
    building_name   VARCHAR(200),
    standard_address TEXT,
    province        VARCHAR(50),
    city            VARCHAR(50),
    district        VARCHAR(50),
    town            VARCHAR(100),
    community       VARCHAR(100),
    adcode          VARCHAR(10),
    lng             DOUBLE PRECISION,
    lat             DOUBLE PRECISION,
    type            VARCHAR(20),
    create_time     TIMESTAMP,
    update_time     TIMESTAMP,
    raw_data        JSONB,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_buildings_district ON buildings(district);
CREATE INDEX IF NOT EXISTS idx_buildings_town ON buildings(town);
CREATE INDEX IF NOT EXISTS idx_buildings_community ON buildings(community);
CREATE INDEX IF NOT EXISTS idx_buildings_update_time ON buildings(update_time);

CREATE TABLE IF NOT EXISTS houses (
    id              VARCHAR(50) PRIMARY KEY,
    building_id     VARCHAR(50) NOT NULL REFERENCES buildings(uid),
    floor_name      VARCHAR(20),
    room            VARCHAR(100),
    address         TEXT,
    name            VARCHAR(200),
    lng             DOUBLE PRECISION,
    lat             DOUBLE PRECISION,
    raw_data        JSONB,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_houses_building_id ON houses(building_id);

-- 爬取检查点，用于断点续传
CREATE TABLE IF NOT EXISTS crawl_checkpoint (
    task_name       VARCHAR(100) PRIMARY KEY,
    status          VARCHAR(20) DEFAULT 'pending',
    total_uids      INTEGER DEFAULT 0,
    processed_uids  INTEGER DEFAULT 0,
    last_error      TEXT,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    extra           JSONB
);
