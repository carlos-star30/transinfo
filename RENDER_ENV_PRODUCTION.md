# Render Production Environment Variables

Use this file as the copy-paste checklist when updating or recreating the Render backend service.

## Current production targets

- Frontend: https://transinfo.netlify.app
- Backend: https://transinfo.onrender.com
- Release: v2.0.0

## Required variables

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
# Optional for Netlify preview deploys:
# CORS_ALLOW_ORIGIN_REGEX=https://.*--transinfo\.netlify\.app
```

Notes:

- `DB_NAME` must match the actual schema already loaded in TiDB. If your current live data is in `test`, keep `DB_NAME=test`.
- The variable name must be exactly `DB_SSL_VERIFY_CERT`. A misspelled key like `DB_SSL_VERIFFY_CERT` will be ignored by the backend.
- `DB_SSL_CA` should use `/etc/ssl/certs/ca-certificates.crt` on Render for this image.

## Render service settings

- Runtime: Docker
- Dockerfile: `backend/Dockerfile`
- Health check path: `/docs`
- Auto deploy: enabled

## Minimal verification after deploy

1. Open `https://transinfo.onrender.com/docs` and confirm FastAPI docs page loads.
2. Open `https://transinfo.netlify.app` and confirm login succeeds.
3. Confirm path search, mapping, export, and import APIs return 200.
4. Confirm the page version badge shows `v2.0.0`.