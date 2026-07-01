-- 深圳统一地址爬虫 v2 数据库 schema
-- 数据来源: https://spatydz.sz.gov.cn/addrdatapc/standard/search/buildingDetail

-- ============================================
-- 1. bd 表（配置+跟踪）：由迁移脚本添加跟踪列
--    
--    migrate.py 会为其增加 crawl_status/error_msg/retry_count/updated_at
-- ============================================

-- ============================================
-- 2. buildings 表（楼栋）
--    数据来源: buildingDetail.result.outerAddressStandard + x/y
-- ============================================
CREATE TABLE IF NOT EXISTS buildings (
    id              VARCHAR(50) PRIMARY KEY,
    sortNum         INTEGER,
    parentId        VARCHAR(50),
    province        VARCHAR(50),
    provinceCode    VARCHAR(10),
    city            VARCHAR(50),
    cityCode        VARCHAR(10),
    county          VARCHAR(50),
    countyCode      VARCHAR(10),
    town            VARCHAR(100),
    townCode        VARCHAR(10),
    community       VARCHAR(100),
    communityCode   VARCHAR(20),
    adcode          VARCHAR(10),
    road            VARCHAR(200),
    roadNo          VARCHAR(50),
    type            VARCHAR(50),
    aoi             VARCHAR(200),
    subAoi          VARCHAR(200),
    building        VARCHAR(200),
    unit            VARCHAR(50),
    floor           VARCHAR(50),
    room            VARCHAR(50),
    aliasList       TEXT,
    address         TEXT,
    aoiId           VARCHAR(50),
    buildingId      VARCHAR(50),
    x               DOUBLE PRECISION,
    y               DOUBLE PRECISION,
    geom            TEXT,
    bgId            VARCHAR(50),
    createBy        VARCHAR(100),
    createTime      VARCHAR(50),
    modifyBy        VARCHAR(100),
    modifyTime      VARCHAR(50),
    source          VARCHAR(50),
    md5Id           VARCHAR(100),
    md5ParentId     VARCHAR(100),
    addrMark        VARCHAR(200),
    businessId      VARCHAR(100),
    businessAddress TEXT,
    businessAddrSrc VARCHAR(100),
    extFields       TEXT,
    updatetime      VARCHAR(50),
    rowColumns      TEXT,
    name            VARCHAR(200),
    housephoto      TEXT,
    qianhaiFlag     VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_buildings_buildingId ON buildings(buildingId);
CREATE INDEX IF NOT EXISTS idx_buildings_adcode ON buildings(adcode);
CREATE INDEX IF NOT EXISTS idx_buildings_community ON buildings(community);

-- ============================================
-- 3. houses 表（房屋）
--    数据来源: buildingDetail.result.floorList[].room[]
-- ============================================
CREATE TABLE IF NOT EXISTS houses (
    id          VARCHAR(50) PRIMARY KEY,
    parentId    VARCHAR(50),
    building    VARCHAR(200),
    room        VARCHAR(100),
    address     TEXT,
    name        VARCHAR(200),
    buildingId  VARCHAR(50),
    x           DOUBLE PRECISION,
    y           DOUBLE PRECISION,
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_houses_buildingId ON houses(buildingId);
