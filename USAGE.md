# 深圳统一地址爬虫 - 用户操作文档

## 一、环境准备

### 1.1 安装依赖

```bash
# 使用国内镜像安装
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

依赖列表：
- `scrapling[all]>=0.4.8` — HTTP 抓取 + 反爬
- `asyncpg>=0.29.0` — 异步 PostgreSQL
- `pyyaml>=6.0` — 配置文件解析
- `aiohttp>=3.9.0` — 监控 HTTP 服务

### 1.2 准备数据库

确保 PostgreSQL 已启动，并存在目标数据库（默认 `sz_address`）。

**要求**：
- 数据库中需有 `bd_csv` 表，包含 `building_code` 字段（用户已提供，64万+ 条）
- 迁移脚本会自动从 `bd_csv` 创建 `bd` 配置表

## 二、数据库迁移

### 2.1 执行迁移

```bash
python db/migrate.py
```

迁移内容：
1. 从 `bd_csv` 创建 `bd` 配置表（含 `crawl_status` 等跟踪字段）
2. 旧 `buildings` → `buildings_old`
3. 旧 `houses` → `houses_old`
4. 创建新 `buildings` 表（38 个字段）
5. 创建新 `houses` 表（9 个字段）

### 2.2 验证迁移

```bash
python -c "import asyncpg, asyncio; 
async def c():
    conn = await asyncpg.connect('postgresql://postgres:YOUR_PASSWORD@localhost:5432/sz_address')
    for t in ['bd','buildings','houses']:
        print(t, await conn.fetchval(f'SELECT COUNT(*) FROM {t}'))
    await conn.close()
asyncio.run(c())
"
```

### 2.3 回滚迁移（如需恢复旧表）

```bash
python db/migrate.py --rollback
```

回滚会删除新表，恢复 `buildings_old` / `houses_old`。

## 三、配置文件说明

配置文件：`config.yaml`

### 3.1 database 段

```yaml
database:
  host: localhost
  port: 5432
  user: postgres
  password: "123456"
  database: sz_address
```

### 3.2 crawler 段

```yaml
crawler:
  concurrency: 2          # 并发 worker 数，保守起步
  batch_size: 1000        # 每批从 bd 表读取多少条
  resume: true            # 基于 bd.crawl_status 断点续传
```

### 3.3 anti_bot 段（反爬策略）

```yaml
anti_bot:
  min_delay: 1.0          # 最小请求间隔（秒）
  max_delay: 3.0          # 最大请求间隔（秒）
  impersonate:            # TLS 指纹轮换列表
    - chrome
    - firefox
    - safari
    - edge
  stealthy_headers: true  # 启用隐身 Headers
  extra_headers:          # 额外请求头，模拟站内来源
    Referer: "https://spatydz.sz.gov.cn/addrdatapc/standard/search"
    Origin: "https://spatydz.sz.gov.cn"
    Accept: "application/json, text/plain, */*"
    Accept-Language: "zh-CN,zh;q=0.9"
    Cache-Control: "no-cache"
    Pragma: "no-cache"
    Sec-Fetch-Dest: "empty"
    Sec-Fetch-Mode: "cors"
    Sec-Fetch-Site: "same-origin"
  proxy: null             # 代理 URL，如: http://user:pass@host:port
  max_retries: 3          # 最大重试次数
  retry_base_delay: 5.0   # 重试基础延迟
  retry_backoff: 2.0      # 重试指数退避倍率
  adaptive_rate: true     # 启用自适应速率
  max_delay_cap: 60.0     # 延迟上限
  success_threshold: 50   # 连续成功 N 次后微微加速
  rate_decrease_factor: 2.0   # 遇限流时延迟倍率
  rate_increase_factor: 0.9   # 持续成功时恢复倍率
```

**注意**：`extra_headers` 会合并到 `stealthy_headers` 生成的浏览器级 Headers 中。只要传入了自定义 `Referer`，Scrapling 默认的 Google Referer 就不会覆盖它。

**调参建议**：
- 首次运行建议 `concurrency=1`, `min_delay=2.0`, `max_delay=5.0`
- 观察 100 条无 403/429 后，再逐步降低 delay 或提高并发
- 出现限流时，程序会自动减速，无需手动干预

### 3.4 monitor 段

```yaml
monitor:
  enabled: true
  host: "0.0.0.0"
  port: 8888
  refresh_interval: 3     # 看板刷新间隔（秒）
```

## 四、启动爬虫

### 4.1 直接启动

```bash
python main.py
```

启动后：
- 控制台输出抓取日志
- 监控页面地址：`http://localhost:8888`
- API 地址：`http://localhost:8888/api/stats`

### 4.2 使用环境变量覆盖配置

```bash
# 覆盖数据库密码
set ADDR_CRAWLER_PASSWORD=yourpassword

# 覆盖并发数
set ADDR_CRAWLER_CONCURRENCY=1

python main.py
```

### 4.3 停止爬虫

按 `Ctrl+C` 即可优雅退出：
- 停止接收新任务
- 等待进行中的请求完成
- 已处理的数据状态写入 `bd` 表
- 重启后自动跳过 `success` 的记录

## 五、查看监控页面

浏览器访问：`http://localhost:8888`

看板展示：
- 运行状态（运行中 / 已完成 / 已暂停）
- 待处理 / 已处理 / 成功 / 失败数量
- 实时速率（req/s）
- 当前请求延迟
- 进度条 + 预计剩余时间
- 最近错误列表
- 楼栋 / 房屋入库数量
- 自适应限速模式提示

## 六、断点续传

爬虫使用数据库 `bd.crawl_status` 作为唯一状态源：

- `pending`：待抓取
- `success`：已成功入库
- `failed`：失败，会按 `max_retries` 重试

**任何中断后**，直接重新运行 `python main.py` 即可从上次停止处继续。

## 七、测试小批量抓取

正式全量抓取前，建议先测试少量数据：

```sql
-- 只保留前 10 条待抓取
UPDATE bd SET crawl_status = 'success';
UPDATE bd SET crawl_status = 'pending'
WHERE building_code IN (
    SELECT building_code FROM bd ORDER BY building_code LIMIT 10
);
```

运行 `python main.py`，观察监控和日志，确认无异常后再全量抓取：

```sql
UPDATE bd SET crawl_status = 'pending', error_msg = NULL, retry_count = 0;
```

## 八、数据表说明

### 8.1 bd 表（配置 + 跟踪）

| 字段 | 说明 |
|------|------|
| building_code | 楼栋编码（主键） |
| crawl_status | pending / success / failed |
| error_msg | 错误信息 |
| retry_count | 重试次数 |
| updated_at | 更新时间 |

### 8.2 buildings 表（楼栋）

数据来源：`buildingDetail.result.outerAddressStandard`

核心字段：`id`, `province`, `city`, `county`, `town`, `community`, `adcode`, `road`, `building`, `address`, `x`, `y` 等。

### 8.3 houses 表（房屋）

数据来源：`buildingDetail.result.floorList[].room[]`

核心字段：`id`, `parentId`, `building`, `room`, `address`, `name`, `buildingId`, `x`, `y`。

## 九、常见问题

### Q1: 运行时报 "relation 'bd' does not exist"

**原因**：未执行数据库迁移。

**解决**：
```bash
python db/migrate.py
```

### Q2: 监控页面无法访问

**原因**：端口冲突或防火墙。

**解决**：
- 修改 `config.yaml` 中 `monitor.port`
- 检查 8888 端口是否被占用

### Q3: 出现大量 403/429

**原因**：请求过快被限流。

**解决**：
- 增大 `anti_bot.min_delay` / `max_delay`
- 降低 `crawler.concurrency`
- 启用 `anti_bot.proxy` 轮换 IP

程序也会自动触发 `adaptive_rate` 减速。

### Q4: 想清空重新抓取

```sql
UPDATE bd SET crawl_status = 'pending', error_msg = NULL, retry_count = 0;
TRUNCATE TABLE buildings, houses RESTART IDENTITY CASCADE;
```

### Q5: 抓取太慢

**建议**：
- 在确认不被限流的前提下，逐步降低 delay
- 提高 concurrency（但不要超过服务器承受）
- 使用代理池分散请求

## 十、文件结构

```
address_crawler/
├── main.py                 # 入口
├── config.yaml             # 配置
├── requirements.txt        # 依赖
├── crawler/
│   ├── client.py           # API 客户端
│   ├── storage.py          # 数据库存储
│   ├── pipeline.py         # 抓取流水线
│   ├── anti_bot.py         # 反爬策略
│   ├── monitor.py          # 监控服务
│   └── enumerator.py       # 旧版枚举器（保留）
├── db/
│   ├── schema_v2.sql       # 新 schema DDL
│   ├── migrate.py          # 迁移脚本
│   └── schema.sql          # 旧 schema DDL
└── templates/
    └── dashboard.html      # 监控看板
```

## 十一、注意事项

1. **先慢后快**：首次运行务必保守，观察反爬响应后再提速
2. **监控优先**：全量抓取时保持监控页面开启
3. **数据库状态是真理**：所有进度以 `bd.crawl_status` 为准
4. **保留旧表**：迁移后的 `buildings_old` / `houses_old` 默认保留，确认无误后再删除
5. **合法合规**：仅抓取授权访问的数据，遵守目标网站 ToS
