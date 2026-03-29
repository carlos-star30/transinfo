# Railway Production Environment Variables

Use this file as a copy-paste checklist when creating the Railway service.

## Required variables

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
DEFAULT_ADMIN_PASSWORD=<strong_admin_password>

AUTH_COOKIE_NAME=trans_fields_mapping_session
AUTH_COOKIE_SECURE=true
AUTH_COOKIE_SAMESITE=none
AUTH_COOKIE_DOMAIN=
```

## Set after Netlify first deploy

```bash
CORS_ALLOW_ORIGINS=https://example-business-app.netlify.app
CORS_ALLOW_ORIGIN_REGEX=https://.*--example-business-app\.netlify\.app
```

## Minimal verification

1. Open `https://example-business-app-api.up.railway.app/docs` and confirm FastAPI docs page loads.
2. Login from the Netlify frontend and check API requests return 200 with cookies.
