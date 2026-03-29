# 示例业务应用部署说明（Netlify + Railway + TiDB）

如果你要直接开始操作，请先看：`DEPLOY_NETLIFY_RAILWAY_TIDB_START_NOW.md`。

目标技术栈：
- 前端：Netlify
- 后端：Railway（Docker）
- 数据库：TiDB Cloud（MySQL 兼容）

## 1. 当前项目的对应关系

当前示例项目已经按这套部署方式整理好：

- 前端发布目录：`frontend-prototype`
- 后端容器入口：根目录 `Dockerfile`
- 运行时 API 配置：`frontend-prototype/runtime-config.js`
- Railway 默认数据库名：`trans_fields_mapping`
- Railway 默认会话名：`trans_fields_mapping_session`

## 2. Railway 需要设置的核心变量

```bash
DB_HOST=<你的_tidb_host>
DB_PORT=4000
DB_USER=<你的_tidb_user>
DB_PASSWORD=<你的_tidb_password>
DB_NAME=trans_fields_mapping

DB_SSL_CA=/etc/ssl/certs/ca-certificates.crt
DB_SSL_DISABLED=false
DB_SSL_VERIFY_CERT=true
DB_SSL_VERIFY_IDENTITY=true

DEFAULT_ADMIN_USERNAME=admin
DEFAULT_ADMIN_PASSWORD=<强密码>

AUTH_COOKIE_NAME=trans_fields_mapping_session
AUTH_COOKIE_SECURE=true
AUTH_COOKIE_SAMESITE=none
AUTH_COOKIE_DOMAIN=

# 首次部署前可先留空
CORS_ALLOW_ORIGINS=
CORS_ALLOW_ORIGIN_REGEX=
```

后端示例域名：`https://example-business-app-api.up.railway.app`

## 3. Netlify 对应设置

- Publish directory: `frontend-prototype`
- Build command: 空

前端示例域名：`https://example-business-app.netlify.app`

## 4. 回填前后端联通配置

在 Railway 中补：

```bash
CORS_ALLOW_ORIGINS=https://example-business-app.netlify.app
# 如需分支预览，可用：
# CORS_ALLOW_ORIGIN_REGEX=https://.*--example-business-app\.netlify\.app
```

在 `frontend-prototype/runtime-config.js` 中补：

```js
window.__DATAFLOW_API_BASE__ = "https://example-business-app-api.up.railway.app";
```

## 5. 联调验收清单

- 前端首页可打开
- 可登录
- 登录后 API 请求返回 200 且带 Cookie
- 搜索与数据流图可用
- 导入功能可正常写入数据库
- `https://example-business-app-api.up.railway.app/docs` 可访问