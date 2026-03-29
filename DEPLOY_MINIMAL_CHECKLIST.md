# 最小部署清单（剔除模板冗余）

本项目由模板初始化而来。部署时请只打包运行必需内容，避免把演示、文档、备份和本地数据一并带入。

## 1. 后端容器最小必需

- `backend/`
- `scripts/`
- `backend/requirements.txt`
- `backend/entrypoint.sh`
- `backend/import_status_api.py`
- `docker-compose.yml`（仅本地/自托管）
- `backend/Dockerfile` 或根 `Dockerfile`
- `.env`（部署平台环境变量等价）

说明：已新增 `.dockerignore`，会自动排除模板与非运行目录。

## 2. 前端静态最小必需（Netlify）

- `frontend-prototype/index.html`
- `frontend-prototype/app.js`
- `frontend-prototype/styles.css`
- `frontend-prototype/runtime-config.js`
- `frontend-prototype/vendor/**`
- `frontend-prototype/Assets/**`

说明：`table-analysis-ui-kit` 的运行时 JS/CSS 已收敛到 `frontend-prototype/vendor/table-analysis-ui-kit/`，不再依赖发布目录外的 `reusable/`。

## 3. 明确排除（不参与部署）

- `backups/`
- `docs/`
- `Excel Data/`
- `Table Definition/`
- `reusable/table-analysis-ui-kit/demo.html`
- `reusable/table-analysis-ui-kit/README.md`
- 根目录各类模板/说明 `DEPLOY_*.md`、`TEMPLATE_INIT.md`、`LOCAL_STARTUP.md`

## 4. 部署前健壮性检查

1. Python 语法检查：
   - `python -m compileall -q backend scripts`
2. 关键健康检查：
   - `scripts/check_backend_health.sh http://127.0.0.1:8000`
3. 运行配置核对：
   - DB 连接环境变量
   - `DEFAULT_ADMIN_PASSWORD`（首次部署且 users 为空时必填，需强密码）
   - `CORS_ALLOW_ORIGINS`
   - `AUTH_COOKIE_*`

## 5. 说明

- `scripts/create_dd03l_table.py` 现已支持在元数据文件缺失时按默认行为“告警并跳过”，避免容器因本地绝对路径缺失直接启动失败。
- 如需强制校验该文件存在，可设置 `DD03L_METADATA_REQUIRED=true`。
- 认证初始化已移除硬编码默认管理员密码；首次引导管理员账号必须由环境变量提供 `DEFAULT_ADMIN_PASSWORD`。
