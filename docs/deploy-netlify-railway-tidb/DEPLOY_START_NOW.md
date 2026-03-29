# Start Now: Deploy 示例业务应用 with Netlify + Railway + TiDB

Target stack:
- Frontend: Netlify
- Backend: Railway (Docker)
- Database: TiDB Cloud

## 1) Backend variables

```bash
DB_HOST=<tidb_host>
DB_PORT=4000
DB_USER=<tidb_user>
DB_PASSWORD=<tidb_password>
DB_NAME=trans_fields_mapping

DB_SSL_CA=/etc/ssl/certs/ca-certificates.crt
DB_SSL_DISABLED=false
DB_SSL_VERIFY_CERT=true
DB_SSL_VERIFY_IDENTITY=true

DEFAULT_ADMIN_USERNAME=admin
DEFAULT_ADMIN_PASSWORD=<strong_password>

AUTH_COOKIE_NAME=trans_fields_mapping_session
AUTH_COOKIE_SECURE=true
AUTH_COOKIE_SAMESITE=none
AUTH_COOKIE_DOMAIN=
```

## 2) Example deployment domains

- Backend: `https://example-business-app-api.up.railway.app`
- Frontend: `https://example-business-app.netlify.app`

## 3) CORS and runtime config

```bash
CORS_ALLOW_ORIGINS=https://example-business-app.netlify.app
```

```js
window.__DATAFLOW_API_BASE__ = "https://example-business-app-api.up.railway.app";
```
