# 环境监测数仓值班工具

面向值班数据工程师的两个网页工具（同一 FastAPI 应用、同一 SQLite 库）：

1. **字段变更核查**（`/`）：原始站点表字段调整上线前，上传「变更前源字段清单」
   「变更后源字段清单」「跨作业字段依赖」三份 JSON，对照两版逐列给出
   **正常 / 断供 / 待核实** 判定和完整依赖路径。
2. **站点观测批次入库**（`/ingest`）：站点观测重投 / 更正 / 撤回事件的批次入库，
   按站点 × 小时查询**有效记录数与数值合计**。

技术栈：FastAPI + SQLite + 原生 HTML/JS 单页，Docker Compose 一键运行。

## 快速开始

```bash
docker compose up --build
# 字段变更核查：http://localhost:8000
# 观测批次入库：http://localhost:8000/ingest
```

核查页可点「载入内置示例核查」一键体验；入库页可点「载入示例批次」，
或下载示例文件修改后上传：`examples/sources_before.json`、`examples/sources_after.json`、
`examples/dependencies.json`、`examples/obs_events.json`。

本地无 Docker 运行：

```bash
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

运行单元测试（仅标准库，无需安装依赖）：

```bash
python3 -m unittest discover -s tests -v
```

## 观测批次入库

### 事件格式

批次文件为事件数组（或 `{"events": [...]}`），每个事件：

```json
{"observation_id": "obs-1001", "revision": 2, "station": "站点A",
 "hour": "2026-11-15T08", "value": 35.2, "op": "upsert"}
{"observation_id": "obs-1004", "revision": 2, "op": "retract"}
```

- `op: "upsert"` 写入 / 更正（`station`/`hour`/`value` 必填，`value` 须为有限数字）；
- `op: "retract"` 撤回（站点/小时/数值可省略）；
- `revision` 为非负整数，重投、更正、撤回都通过修订号表达先后。

### 入库口径

- **最高修订生效**：同一观测跨批次只采用最高修订号的记录，旧修订一律忽略，
  不得回滚已有结果（撤回也是墓碑，能挡住迟到的旧修订，更高修订可重新写入）；
- **同修订幂等**：同修订号重投且内容一致（操作、站点、小时、数值完全相同）
  只计一次；**内容冲突则整批拒绝**，错误信息指出冲突的观测ID；
- **先扣后计**：撤回、或更正改变了站点/小时/数值时，先从原 (站点, 小时)
  汇总中扣除旧值，再计入新值；
- **整批事务**：每批在一个 `BEGIN IMMEDIATE` 事务内应用——两批同时上传串行生效，
  互不丢彼此的有效修订；汇总只在提交时变化，查询只读到完整已发布的结果，
  失败批次（格式错误 400 / 内容冲突 409）保留旧结果并在批次历史中留痕。

### 查询

入库页展示各站点 × 小时的有效记录数与数值合计（可按站点过滤），
以及最近 20 条批次历史（含每批生效/撤回/去重/旧修订统计与失败原因）。

## 字段变更核查：判定口径

每个产出列允许 **多组输入**：

- **组内全部输入可用，该组才可用；任一组可用即可产出** → `正常`
  （主用组失效但有完整备用组时，仍然正常，判定说明会标注「含备用组」）
- 所有输入组都确定不可用（源字段缺失、或上游列自身已断供）→ `断供`
- 仅能依靠 **缺失引用**（依赖清单中不存在的作业/列、既非源表也非作业的引用）
  或 **无源支撑的循环** 才能产出 → `待核实`

源字段缺失时会对照另一版清单标注「本版已移除（对照版存在）」还是「两版均无」。
判定沿依赖链传播：上游断供会导致下游断供；上游待核实只会让下游待核实，不会升级为断供。
「最终列」= 未被任何其他产出列消费的列；`external` 标记的列视为对外数据集列，页面默认聚焦这两类。

## 版本与一致性

- 核查（解析 + 评估）全部在写库前完成，**核查进行中旧版照常可查**；
- 成功后在一个事务内整版替换（旧版归档），**失败保留旧版**并在历史中留痕；
- 每个快照自包含三份输入与判定报告，查询只读 `active` 的唯一版本，**绝不混用两版**；
- SQLite 开启 WAL，写入期间并发读取不受阻塞。

## 输入文件格式

**源字段清单**（变更前/后各一份，三种写法等价）：

```json
{ "tables": { "raw_air_station_hourly": ["station_id", "pm25"] } }
{ "raw_air_station_hourly": ["station_id", "pm25"] }
[ "raw_air_station_hourly.station_id", "raw_air_station_hourly.pm25" ]
```

**跨作业字段依赖**：

```json
{
  "jobs": [
    {
      "name": "job_air_quality_hourly",
      "outputs": {
        "wind_speed_avg": [
          ["raw_air_station_hourly.wind_speed"],
          ["job_meteo_clean.wind_speed"]
        ]
      }
    }
  ],
  "external": ["job_daily_report.report_wind"]
}
```

- `outputs.列名` 的值是 **组的数组**：外层每一项是一组备用输入，组内是所有必须同时可用的输入；
  简写 `"col": ["t.a", "t.b"]`（元素非数组）等价于单组 `[["t.a", "t.b"]]`。
- 输入引用 `"属主.名"` 自动判定：属主是已定义作业 → 上游列；是源表 → 源字段；都不是 → 缺失引用（待核实）。
  同名歧义时可用 `{"source": "t.c"}` 或 `{"column": "j.c"}` 强制指定。
- `external`（可选）列出对外数据集列，必须引用已定义的产出列。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/snapshots` | multipart 上传 `sources_before` / `sources_after` / `dependencies` + 可选 `name`；成功整版发布，失败 400 且保留旧版 |
| GET | `/api/snapshots/current` | 现役版本全量内容（输入 + 判定报告） |
| GET | `/api/snapshots` | 版本历史（含失败记录） |
| POST | `/api/observations/batches` | multipart 上传 `events_file`（观测事件批次 JSON）+ 可选 `name`；成功 201 整批生效，格式错误 400 / 同修订内容冲突 409，失败均保留旧汇总并留痕 |
| GET | `/api/observations/summary` | 各站点 × 小时的有效记录数与数值合计，可选 `?station=` 精确过滤 |
| GET | `/api/observations/batches` | 批次历史（含统计与失败原因） |
| GET | `/examples/{name}` | 示例文件下载 |
| GET | `/` `/ingest` | 核查页面 / 批次入库页面 |

## 目录结构

```
app/
  main.py            FastAPI 入口与接口（核查 + 批次入库）
  evaluator.py       核查：解析 + 三值判定 + 依赖路径生成（核心，纯标准库）
  observations.py    入库：事件解析校验 + 批次事务化应用（核心，纯标准库）
  store.py           SQLite 快照存储（整版原子替换、失败留痕、WAL）
  static/index.html  核查页面
  static/ingest.html 批次入库页面
examples/            本地示例（核查三件套 + obs_events.json 示例批次）
tests/               单元测试（evaluator 19 例 + observations 19 例）
Dockerfile / compose.yaml / requirements.txt
```
