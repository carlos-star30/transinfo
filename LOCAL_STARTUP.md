# 本地启动文档（当前配置）

本文档固定使用项目当前生效的本地配置，目标是每次都按同一套参数启动，避免端口和代理不一致导致登录失败。

## 对话口令约定

以后建议固定使用下面两句话，不再说模糊的“启动项目”：

- `启动本地项目`：表示本地前端 + 本地后端 + 本地数据库。
- `检查线上项目`：表示只检查已部署环境，不启动本地服务，不切本地数据库。

如果你明确说 `本地前端连线上后端`，才会进入混合模式；否则默认不这么做。

## 当前固定配置

- 前端代理地址: http://localhost:8088
- 前端代理监听地址环境变量: `DATAFLOW_DEV_HOST`，默认 `127.0.0.1`
- 后端 API 地址: http://127.0.0.1:8000
- 前端代理脚本: `frontend-prototype/dev_proxy_server.py`
- 后端启动脚本: `scripts/run_import_status_api.sh`
- 后端入口模块: `backend.import_status_api:app`

补充文档:

- Windows + SQLite 本地启动: `SQLITE_LOCAL_STARTUP.md`
- 路径逻辑与表关系: `docs/path-selection-logic-and-table-relations.md`

## 一次性准备

在项目根目录执行:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

## 一键启动（Mac + Parallels）

在项目根目录执行:

```bash
./scripts/start_local_mac_parallels.sh
```

脚本会自动:

- 启动后端 `8000`，如果已在运行则跳过
- 启动前端代理 `8088`，监听 `0.0.0.0` 以便 Parallels Win11 访问
- 等待服务就绪后打印 Mac 和 Win11 可访问地址

当前这台机器在 Parallels Shared Network 下的 Win11 访问地址是:

```text
http://10.211.55.2:8088/
```

## 启动顺序（每次都这样）

建议开两个终端，按顺序启动。

### 终端 1：启动后端（8000）

```bash
cd "/Volumes/Data/VS Code/Transformation fields mapping"
source .venv/bin/activate
HOST=0.0.0.0 PORT=8000 ./scripts/run_import_status_api.sh
```

看到 uvicorn running on 0.0.0.0:8000 即表示后端已启动。

### 终端 2：启动前端代理（8088）

```bash
cd "/Volumes/Data/VS Code/Transformation fields mapping"
source .venv/bin/activate
DATAFLOW_DEV_HOST=127.0.0.1 DATAFLOW_DEV_PORT=8088 DATAFLOW_BACKEND_BASE=http://127.0.0.1:8000 \
  python frontend-prototype/dev_proxy_server.py
```

看到 Serving local frontend proxy at http://localhost:8088 即表示前端代理已启动。

如果需要从 Parallels Windows 11 访问，把监听地址改为 `0.0.0.0`:

```bash
cd "/Volumes/Data/VS Code/Transformation fields mapping"
source .venv/bin/activate
DATAFLOW_DEV_HOST=0.0.0.0 DATAFLOW_DEV_PORT=8088 DATAFLOW_BACKEND_BASE=http://127.0.0.1:8000 \
  python frontend-prototype/dev_proxy_server.py
```

在当前这台 Mac 的 Parallels Shared Network 下，Windows 11 可访问:

```text
http://10.211.55.2:8088/
```

### 浏览器访问

打开:

```text
http://localhost:8088/
```

如果是 Parallels Windows 11，请打开:

```text
http://10.211.55.2:8088/
```

## 快速自检

### 检查端口是否监听

```bash
lsof -nP -iTCP:8000 -sTCP:LISTEN
lsof -nP -iTCP:8088 -sTCP:LISTEN
```

两条命令都有输出才算完整启动。

### 检查后端健康

```bash
./scripts/check_backend_health.sh http://127.0.0.1:8000
```

## 常见问题

### 登录失败：`urlopen error [Errno 61] Connection refused`

通常是前端已开、后端未开，或后端端口不是 8000。

处理步骤:

1. 先确认 `lsof -nP -iTCP:8000 -sTCP:LISTEN` 有输出。
2. 如无输出，按上文“终端 1”重新启动后端。
3. 保持前端代理的 `DATAFLOW_BACKEND_BASE=http://127.0.0.1:8000` 不变。

### 8088 打不开

通常是前端代理未启动，或被其他进程占用。

处理步骤:

1. `lsof -nP -iTCP:8088 -sTCP:LISTEN` 查看占用。
2. 如被其他进程占用，先停止该进程后重启前端代理。

### 后端启动后报数据库连接错误

`backend/import_status_api.py` 的默认数据库参数是:

- host: localhost
- port: 3306
- user: root
- password: showlang
- database: trans_fields_mapping

请确保本地 MySQL 使用这一组参数，或在启动后端前通过环境变量覆盖。

### 切换 SQLite 启动（无需 MySQL）

后端支持通过环境变量切到 SQLite:

```bash
DB_DRIVER=sqlite SQLITE_DB_PATH="./backend/data/trans_fields_mapping.db" HOST=0.0.0.0 PORT=8000 ./scripts/run_import_status_api.sh
```

若需要把现有 MySQL 数据搬到 SQLite，先执行:

```bash
python scripts/migrate_mysql_to_sqlite.py --source-host localhost --source-port 3306 --source-user root --source-password showlang --source-database trans_fields_mapping --target-file backend/data/trans_fields_mapping.db --drop-existing
```

Windows PowerShell 示例:

```powershell
$env:DB_DRIVER="sqlite"
$env:SQLITE_DB_PATH="backend/data/trans_fields_mapping.db"
C:/Users/JINYOZH/AppData/Local/Python/pythoncore-3.14-64/python.exe -m uvicorn backend.import_status_api:app --host 0.0.0.0 --port 8000 --reload
```

## 停止服务

在两个终端分别按 `Ctrl+C`。
