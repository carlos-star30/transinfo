# 示例业务应用部署说明（Netlify + Railway + TiDB）

如果你要直接开始操作，请先看：`DEPLOY_START_NOW.md`。

## 核心示例值

- 数据库名：`trans_fields_mapping`
- 会话名：`trans_fields_mapping_session`
- Railway 域名示例：`https://example-business-app-api.up.railway.app`
- Netlify 域名示例：`https://example-business-app.netlify.app`

## 部署结构

- 前端：Netlify，发布 `frontend-prototype`
- 后端：Railway，使用根目录 `Dockerfile`
- 数据库：TiDB Cloud

## Railway 最小必填变量

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

CORS_ALLOW_ORIGINS=https://example-business-app.netlify.app
```

## 前端运行时配置

```js
window.__DATAFLOW_API_BASE__ = "https://example-business-app-api.up.railway.app";
```

## 验收检查

- 首页可打开
- 可登录
- 请求返回 200 且带 Cookie
- 搜索与数据流图可用
- `/docs` 可访问