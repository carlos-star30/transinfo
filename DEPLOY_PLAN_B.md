# Current Production Deployment (Netlify + Render + TiDB)

This is the current recommended deployment path for this repository.

## 0) Architecture

- Frontend: Netlify
- Backend API: Render Web Service (Docker)
- Database: TiDB Cloud Serverless or another MySQL-compatible managed database

## 1) Example identity values

- Database: `trans_fields_mapping`
- Frontend URL example: `https://transinfo.netlify.app`
- Backend URL example: `https://transinfo.onrender.com`
- Current release target: `v2.0.0`

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
DB_PORT=4000
DB_USER=<tidb_user>
DB_PASSWORD=<tidb_password>
DB_NAME=<actual_database_name>

DB_SSL_CA=/etc/ssl/certs/ca-certificates.crt
DB_SSL_DISABLED=false
DB_SSL_VERIFY_CERT=true
DB_SSL_VERIFY_IDENTITY=true

DEFAULT_ADMIN_USERNAME=admin
DEFAULT_ADMIN_PASSWORD=<strong_admin_password>

AUTH_COOKIE_NAME=transinfo_session
AUTH_COOKIE_SECURE=true
AUTH_COOKIE_SAMESITE=none
AUTH_COOKIE_DOMAIN=

CORS_ALLOW_ORIGINS=https://transinfo.netlify.app
# Preview deploys if needed:
# CORS_ALLOW_ORIGIN_REGEX=https://.*--transinfo\.netlify\.app
```

Important:

- If the current TiDB schema name is `test`, do not change `DB_NAME`; keep it as `test`.
- The SSL verify variable must be spelled `DB_SSL_VERIFY_CERT`.
- On Render, prefer `DB_SSL_CA=/etc/ssl/certs/ca-certificates.crt`.

## 4) Netlify runtime config

After first deployment, update `frontend-prototype/runtime-config.js` to:

```js
window.__DATAFLOW_API_BASE__ = "https://transinfo.onrender.com";
```

## 5) Verification

- Open the Netlify site
- Log in with the admin account
- Confirm search and flow pages work
- Confirm API calls include cookies and return 200

## 6) Files to maintain for next deployment

- `netlify.toml`
- `render.yaml`
- `frontend-prototype/runtime-config.js`
- `RENDER_ENV_PRODUCTION.md`
- `RELEASE_v2.0.0.md`
