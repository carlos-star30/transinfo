# Deployment Plan B (Cloudflare Pages + Render + TiDB Cloud)

This guide is the alternative deployment path for long-running free validation without Oracle Cloud.

## 0) Architecture

- Frontend: Cloudflare Pages
- Backend API: Render Web Service (Docker)
- Database: TiDB Cloud Serverless

## 1) Example identity values

- Database: `trans_fields_mapping`
- Backend URL example: `https://example-business-app-api.onrender.com`

## 2) TiDB setup

Create one database:

- `trans_fields_mapping`

Collect:

- host
- port
- username
- password
- database name

## 3) Render environment variables

```bash
DB_HOST=<tidb_host>
DB_PORT=<tidb_port>
DB_USER=<tidb_user>
DB_PASSWORD=<tidb_password>
DB_NAME=trans_fields_mapping

DB_SSL_CA=/etc/ssl/certs/ca-certificates.crt
DB_SSL_DISABLED=false
DB_SSL_VERIFY_CERT=true
DB_SSL_VERIFY_IDENTITY=true

DEFAULT_ADMIN_USERNAME=admin
DEFAULT_ADMIN_PASSWORD=<strong_admin_password>

CORS_ALLOW_ORIGINS=https://<your-pages-project>.pages.dev
AUTH_COOKIE_SECURE=true
AUTH_COOKIE_SAMESITE=none
AUTH_COOKIE_DOMAIN=
```

## 4) Cloudflare Pages runtime config

After first deployment, update `frontend-prototype/runtime-config.js` to:

```js
window.__DATAFLOW_API_BASE__ = "https://example-business-app-api.onrender.com";
```

## 5) Verification

- Open the Pages site
- Log in with the admin account
- Confirm search and flow pages work
- Confirm API calls include cookies and return 200
