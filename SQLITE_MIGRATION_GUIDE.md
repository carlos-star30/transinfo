# SQLite 迁移指南

本项目已支持从 MySQL 迁移到 SQLite 进行本地开发或部署。

## 快速开始（无 MySQL）

### Step 1: 安装依赖

```bash
pip install sqlite3  # 通常 Python 内置
pip install -r backend/requirements.txt
```

### Step 2: 启动后端（SQLite 模式）

Windows PowerShell:

```powershell
$env:DB_DRIVER="sqlite"
$env:SQLITE_DB_PATH="backend/data/trans_fields_mapping.db"
$env:DEFAULT_ADMIN_USERNAME="admin"
$env:DEFAULT_ADMIN_PASSWORD="Admin@123"
C:/Users/JINYOZH/AppData/Local/Python/pythoncore-3.14-64/python.exe -m uvicorn backend.import_status_api:app --host 0.0.0.0 --port 8000
```

macOS/Linux:

```bash
export DB_DRIVER=sqlite
export SQLITE_DB_PATH=backend/data/trans_fields_mapping.db
export DEFAULT_ADMIN_USERNAME="admin"
export DEFAULT_ADMIN_PASSWORD="Admin@123"
python -m uvicorn backend.import_status_api:app --host 0.0.0.0 --port 8000
```

### Step 3: 启动前端代理

在新终端中：

```bash
cd "directory/to/project"
export DATAFLOW_DEV_HOST=127.0.0.1
export DATAFLOW_DEV_PORT=8088
export DATAFLOW_BACKEND_BASE=http://127.0.0.1:8000
python frontend-prototype/dev_proxy_server.py
```

### Step 4: 访问主页

打开浏览器，访问 http://localhost:8088/

## 从现有 MySQL 迁移数据

如果你有现有的 MySQL 数据库并想迁移到 SQLite：

### 运行迁移脚本

```bash
python scripts/migrate_mysql_to_sqlite.py \
  --source-host localhost \
  --source-port 3306 \
  --source-user root \
  --source-password showlang \
  --source-database trans_fields_mapping \
  --target-file backend/data/trans_fields_mapping.db \
  --drop-existing
```

迁移脚本会：
- 自动创建 `backend/data/` 目录
- 建立 SQLite 表结构
- 批量导入所有 MySQL 数据
- 详细打印迁移进度

**约 500MB 数据预计需要 10-15 分钟**

### 迁移特定表

若只想迁移部分表：

```bash
python scripts/migrate_mysql_to_sqlite.py \
  --source-host localhost \
  --source-port 3306 \
  --source-user root \
  --source-password showlang \
  --source-database trans_fields_mapping \
  --table rstran \
  --table dd03l \
  --target-file backend/data/trans_fields_mapping.db
```

## 环境变量说明

### DB_DRIVER

- `mysql` (默认): 使用 MySQL 数据库
- `sqlite`: 使用 SQLite 数据库

### SQLITE_DB_PATH

SQLite 数据库文件路径，默认：`backend/data/trans_fields_mapping.db`

### MySQL 环境变量

- `DB_HOST` (默认: `localhost`)
- `DB_PORT` (默认: `3306`)
- `DB_USER` (默认: `root`)
- `DB_PASSWORD` (默认: `showlang`)
- `DB_NAME` (默认: `trans_fields_mapping`)

### 认证环境变量

- `DEFAULT_ADMIN_USERNAME` (默认: `admin`)
- `DEFAULT_ADMIN_PASSWORD` (默认: 空，首次启动**必须设置**，格式: ` Min 8 chars, 1 lower, 1 upper, 1 digit, 1 special`)
- `AUTH_SESSION_HOURS` (默认: `12`)

## 常见问题

### Q: SQLite 和 MySQL 哪些功能不兼容？

A: 大多数查询和导入功能完全兼容。只有以下 MySQL 特有功能暂不支持：
- `SHOW COLUMNS` 用法变为 `PRAGMA table_info()`
- `UNSIGNED` 数据类型转为 `INTEGER`
- `ON DUPLICATE KEY UPDATE` 改为 `INSERT OR REPLACE`

所有这些都已在后端代码中自动适配。

### Q: SQLite 性能如何？

A: 
- 50 万行以下：完全兼容，速度可能更快（本地 I/O）
- 500 万行+：可能需要考虑回到 MySQL（SQLite 单文件限制）

当前项目的 `rstran` 等表通常 < 100 万行，SQLite 足够。

### Q: 能再切回 MySQL 吗？

A: 可以。只需改回 `DB_DRIVER=mysql` 并重启后端。SQLite 和 MySQL 数据独立存储，互不影响。

### Q: SQLite 文件备份？

A: 直接复制 `backend/data/trans_fields_mapping.db` 即可，单文件自包含。

## 故障排除

### 启动时报 `SQLITE_DB_PATH not found`

SQLite 驱动会自动创建目录。如果报错，检查路径权限：

```bash
mkdir -p backend/data
chmod 755 backend/data
```

### 数据库锁定错误

SQLite 在并发写入时可能锁定。解决：

1. 确保只有一个后端进程在运行
2. 检查是否有其他进程打开了 `.db` 文件（IDE 调试器等）

### 导入失败：类型不匹配

SQLite 类型系统较宽松，但某些插入可能失败。检查：

```bash
python scripts/migrate_mysql_to_sqlite.py --source-database trans_fields_mapping --batch-size 100
```

小批量导入便于定位问题。

## 更多信息

- 后端驱动逻辑: [backend/import_status_api.py](backend/import_status_api.py#L1)
- 迁移脚本: [scripts/migrate_mysql_to_sqlite.py](scripts/migrate_mysql_to_sqlite.py)
- 原 MySQL 启动文档: [LOCAL_STARTUP.md](LOCAL_STARTUP.md)
