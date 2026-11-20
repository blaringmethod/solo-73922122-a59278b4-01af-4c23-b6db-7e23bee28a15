# 观测数据 SQL 清洗沙箱

面向采样数据工程师的外协清洗 SQL 试跑平台：上传观测 CSV、按站点提交只读
SQL，成功后下载结果 CSV 交给质控；质控员也可勾选「同时生成 Parquet」，
让每轮同时产出可直接入仓的 Parquet 文件。基于 **FastAPI + DuckDB**，
全部数据驻留内存，每轮独立隔离执行。

## 功能与约束（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 上传至多 1 MiB 观测 CSV（站号、UTC时刻、温度） | `POST /api/upload`，超限返回 413；表头支持任意列序，UTF-8 编码 |
| 选站后提交 SQL，每轮输入固定为所选站点数据 | 每轮仅把该站点行装入该轮**私有 DuckDB 连接**的 `observations` 表 |
| 仅允许 SELECT / WITH，支持筛选、连接、聚合 | 静态 SQL 守卫（sqlparse 单语句判定 + 关键字/模式黑名单） |
| 不得读写服务器文件、联网、加载扩展 | 守卫拦截 `read_csv`/`COPY`/`ATTACH`/`LOAD`/`INSTALL`/`http_get` 等；DuckDB 连接再以 `enable_external_access=false` 兜底关闭文件系统、网络与扩展 |
| 不得访问其他轮次数据 | 每轮独立线程 + 独立内存库 + 独立数据副本，其他轮次的连接不可达 |
| 最多两轮并行，满额拒绝 | 注册表实时统计活动轮次，第 3 个请求返回 503 |
| 执行满 5 秒终止 | 看门狗在 5 秒调用 DuckDB 原生 `interrupt()` 立即中止，状态置 `failed` |
| 结果超过 8 MiB 整轮失败 | 流式累计结果字节，超限即失败，已生成内容全部丢弃 |
| 勾选「同时生成 Parquet」交付入仓结果 | 提交时 `with_parquet=true`（网页有同名复选框），成功后同时开放 CSV 与 Parquet 下载 |
| 每轮 SQL 只执行一次，两份文件同源 | 单个游标流式拉取，同一批行分别写入 CSV 文本与内存 Parquet writer，列名、列序、行序、重复行完全一致 |
| Parquet 保留类型与精度 | BOOLEAN→bool、INTEGER→int32、BIGINT→int64、DOUBLE→double、DECIMAL(p,s)→decimal128(p,s)、DATE→date32、TIMESTAMP→timestamp[us]、VARCHAR→string；NULL 与空字符串严格区分 |
| 列名重复或类型不支持时指出对应列，整轮失败 | 执行后先校验结果模式：重复列名或非上述类型的列会在错误信息中指名，轮次置 `failed`，无任何下载 |
| 两份文件合计限 8 MiB | 逐批累计 `CSV 字节 + Parquet 缓冲字节`，超限即中止；收尾按最终文件大小再校验一次 |
| 全部生成成功后才同时开放下载 | 两份文件都完整写出后轮次才置 `succeeded`，两个下载链接同时可用 |
| 生成失败、超限、超时或取消时均无下载 | 非 `succeeded` 轮次两个下载接口一律返回 409，内存中的结果一并清空 |
| 保留普通 CSV 试算 | 不勾选时行为与之前完全一致（仅 CSV、限 8 MiB、重复列名不拦截） |
| 可查询状态、可取消 | `GET /api/rounds/{id}`、`POST /api/rounds/{id}/cancel` |
| 取消受理不等执行结束；实际退出后才报取消并释放名额；迟到结果不开放 | 取消立即返回 `accepted`，监督线程等 worker 真正退出后才置 `cancelled`；取消优先于同时到达的结果 |
| 成功先发布则不可取消 | 已 `succeeded` 的轮次取消返回 409，结果仍可下载 |
| 失败/超时不供部分结果 | 失败轮次结果接口返回 409 |
| 慢查询不挡另一轮、状态查询、已有结果下载 | 每轮独立线程；DuckDB 执行 C++ 期间释放 GIL 真并行；HTTP 接口均为短操作 |

## 目录结构

```
app/
  config.py       限额常量（1 MiB / 5 s / 8 MiB / 2 并行）
  csv_io.py       CSV 解析与校验
  sql_guard.py    SQL 静态安全校验
  parquet_io.py   Parquet 模式校验（重复列/类型白名单）与流式内存写出
  worker.py       每轮查询执行器（独立 DuckDB 连接、interrupt 中止、8MiB 判定、双格式串行化）
  registry.py     轮次注册表、名额控制、状态机、看门狗与监督线程
  store.py        内存数据集（按站点切分）
  main.py         FastAPI 路由
  static/index.html  演示网页（上传/选站/提交/轮询/取消/下载 CSV 与 Parquet）
sample_data/observations.csv  演示数据（3 个站点 9 行）
tests/            84 个 pytest 用例（SQL 守卫、CSV、Parquet、端到端 API）
Dockerfile, docker-compose.yml
```

## 本地运行（无需 Docker）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 浏览器打开 http://localhost:8000
```

## Docker Compose 运行

```bash
docker compose up --build
# 打开 http://localhost:8000
```

Compose 额外做了加固：只读根文件系统、`no-new-privileges`、丢弃所有 Linux
capability、512 MiB 内存上限、内置健康检查。服务本身不写任何磁盘文件。

## HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 演示网页 |
| GET | `/api/health` | 健康状态、当前活动轮数、站点列表 |
| POST | `/api/upload` | multipart 上传 CSV（≤1 MiB） |
| POST | `/api/rounds` | 提交 `{station_id, sql, with_parquet?}` 创建一轮；满额 503、SQL 非法 400 |
| GET | `/api/rounds` / `/api/rounds/{id}` | 轮次列表 / 单轮状态 |
| POST | `/api/rounds/{id}/cancel` | 请求取消（成功轮返回 409） |
| GET | `/api/rounds/{id}/result` | 下载结果 CSV（仅 succeeded 可用） |
| GET | `/api/rounds/{id}/result.parquet` | 下载结果 Parquet（仅 succeeded 且勾选 Parquet 的轮次可用，否则 409/404） |

轮次状态：`pending` → `running` → `succeeded` / `failed` / `cancelled`。

## Parquet 交付说明

* 网页勾选「同时生成 Parquet」或 API 传 `with_parquet: true` 后，该轮
  SQL 仍只执行一次：worker 从同一个游标逐批取行，同时写 CSV 与内存
  Parquet（snappy 压缩），两份文件的列名、列序、行序、重复行完全一致。
* 支持的列类型：BOOLEAN、INTEGER、BIGINT、DOUBLE、DECIMAL(p,s)、DATE、
  TIMESTAMP、VARCHAR；其他类型（如 HUGEINT、BLOB、INTERVAL、TIME、
  TIMESTAMPTZ）或重复列名会使整轮失败，错误信息指明具体列。
* Parquet 中 NULL 与空字符串是两个不同的值。
* 两份文件合计 ≤ 8 MiB；任一份生成失败、超限、超时或被取消时，两个
  下载接口都不可用（409）。

## 测试

```bash
pip install pytest httpx
python -m pytest tests/ -q
```

## 设计说明：为什么用线程 + interrupt 而非子进程

首选方案本为每轮独立子进程，以便从进程边界硬终止。当前构建/运行环境的
seccomp 策略禁止向子进程发送信号（`SIGTERM/SIGKILL` 均返回 EPERM），硬杀
进程不可行。因此改为：每轮一个守护线程持有**独立的内存 DuckDB 连接**，
超时/取消时调用 DuckDB 官方的 `connection.interrupt()`，查询在 C++ 层立即
抛 `InterruptException` 退出（已实测中断延迟 < 10 ms）。轮次间无共享连接、
无共享表，数据隔离不依赖进程边界；DuckDB 执行期间释放 GIL，两个慢查询可
真正并行。
