# Start Now: Deploy 示例业务应用 with Netlify + Railway + TiDB

Target stack:
- Frontend: Netlify
- Backend: Railway (Docker)
- Database: TiDB Cloud

## 1) Prepare Git source

1. Push this project to GitHub.
2. Keep branch `main` as the deployment branch.

## 2) Deploy backend on Railway

1. Railway -> New Project -> Deploy from GitHub Repo.
2. Select this repository.
3. Railway auto-detects the root `Dockerfile`.
4. Set service environment variables:

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

# Fill this after Netlify is deployed
CORS_ALLOW_ORIGINS=
CORS_ALLOW_ORIGIN_REGEX=
```

5. Deploy and record the backend URL, for example:
   - `https://example-business-app-api.up.railway.app`

## 3) Deploy frontend on Netlify

1. Netlify -> Add new site -> Import an existing project.
2. Select the same GitHub repository.
3. Netlify will use `netlify.toml` automatically:
   - Build command: empty
   - Publish directory: `frontend-prototype`
4. Deploy and record the frontend URL, for example:
   - `https://example-business-app.netlify.app`

## 4) Backfill CORS and API base

1. In Railway, set:

```bash
CORS_ALLOW_ORIGINS=https://example-business-app.netlify.app
```

2. Update `frontend-prototype/runtime-config.js`:

```js
window.__DATAFLOW_API_BASE__ = "https://example-business-app-api.up.railway.app";
```

3. Commit and push. Netlify will redeploy automatically.

## 5) Verify

1. Open the Netlify URL.
2. Log in with the admin account.
3. Confirm API calls return 200 and include cookies.
4. Check backend docs at `https://example-business-app-api.up.railway.app/docs`.
