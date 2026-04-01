# 本地 SQLite 启动指南

本文档仅描述当前项目真实可用的 SQLite 本地启动方式。当前后端入口不是 auth_server，而是 backend.import_status_api:app；数据库开关也不是 DB_TYPE，而是 DB_DRIVER。

## 对话口令约定

以后如果你要我直接操作，请优先使用这两句：

- `启动本地项目`
- `检查线上项目`

第一句会按本文档启动本地前后端与本地 SQLite；第二句只做线上检查，不启动本地服务。

## 当前固定配置

- 前端地址: http://localhost:8088
- 后端地址: http://127.0.0.1:8000
- 前端脚本: frontend-prototype/dev_proxy_server.py
- 后端入口: backend.import_status_api:app
- SQLite 文件: backend/data/trans_fields_mapping.db

## 推荐启动顺序

建议开两个终端，先启动后端，再启动前端。

如果你在 Windows 本地使用，优先直接用这两个固定脚本，避免环境变量残留到线上配置：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start_local_sqlite_backend.ps1
powershell -ExecutionPolicy Bypass -File scripts/start_local_frontend.ps1
```

### 终端 1：启动 SQLite 后端

PowerShell：

```powershell
$env:DB_DRIVER="sqlite"
$env:SQLITE_DB_PATH="backend/data/trans_fields_mapping.db"
C:/Users/JINYOZH/AppData/Local/Python/pythoncore-3.14-64/python.exe -m uvicorn backend.import_status_api:app --host 0.0.0.0 --port 8000 --reload
```

如果你已经激活虚拟环境，也可以直接执行：

```powershell
$env:DB_DRIVER="sqlite"
$env:SQLITE_DB_PATH="backend/data/trans_fields_mapping.db"
python -m uvicorn backend.import_status_api:app --host 0.0.0.0 --port 8000 --reload
```

看到 Uvicorn 监听 8000 即表示后端已启动。

### 终端 2：启动前端代理

在项目根目录执行：

```powershell
$env:DATAFLOW_DEV_HOST="127.0.0.1"
$env:DATAFLOW_DEV_PORT="8088"
$env:DATAFLOW_BACKEND_BASE="http://127.0.0.1:8000"
python frontend-prototype/dev_proxy_server.py
```

看到 Serving local frontend proxy at http://localhost:8088 即表示前端已启动。

## 防串线说明

当前前端已增加本地防串线逻辑：如果页面运行在 localhost、127.0.0.1 或 file 模式下，而 runtime-config.js 里仍然写着线上 API 地址，则前端会忽略这个线上地址，优先回到当前本地页面来源。

这可以避免“项目已经部署过，结果本地打开时又连到了线上后端”的混用问题。

浏览器打开：

```text
http://localhost:8088/
```

## 初始化与数据

本仓库当前已经在使用 backend/data/trans_fields_mapping.db 作为本地 SQLite 数据文件。若你需要从 MySQL 重建 SQLite，可执行：

```powershell
python scripts/migrate_mysql_to_sqlite.py --source-host localhost --source-port 3306 --source-user root --source-password showlang --source-database trans_fields_mapping --target-file backend/data/trans_fields_mapping.db --drop-existing
```

若只是初始化最小 SQLite 结构，可执行：

```powershell
python scripts/init_sqlite_db.py
```

## 快速自检

检查 8000 / 8088 端口：

```powershell
netstat -ano | findstr ":8000"
netstat -ano | findstr ":8088"
```

检查后端健康：

```powershell
bash scripts/check_backend_health.sh http://127.0.0.1:8000
```

如果本机没有 bash，也可以直接访问：

```text
http://127.0.0.1:8000/health
```

## 常见问题

### 登录失败或接口 502

通常是前端已经启动，但后端 8000 没有起来，或者 DATAFLOW_BACKEND_BASE 指到了错误地址。

排查顺序：

1. 先确认 8000 端口已监听。
2. 再确认前端环境变量 DATAFLOW_BACKEND_BASE=http://127.0.0.1:8000。
3. 确认后端使用的是 backend.import_status_api:app，而不是 backend.auth_server:app。

### SQLite 启动后查不到数据

通常是 SQLITE_DB_PATH 指向了错误文件，或误用了新建的空库。

建议固定使用：

```text
backend/data/trans_fields_mapping.db
```

### 为什么不要再用 DB_TYPE

当前代码读取的是 DB_DRIVER。继续设置 DB_TYPE 不会切换到 SQLite。

## 对应文档

- 通用本地启动说明：LOCAL_STARTUP.md
- SQLite 迁移说明：SQLITE_MIGRATION_GUIDE.md
- 路径逻辑与表关系说明：docs/path-selection-logic-and-table-relations.md
