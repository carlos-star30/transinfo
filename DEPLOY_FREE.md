# Free Deployment Guide (No Custom Domain)

This guide gives you a long-term free setup with URL access.

Current stable release: v2.0.0

This v2.0.0 release includes the stabilized path-selection flow, uppercase technical-name alignment, field metadata display, RSTRANT transformation text support, aligned export improvements, and the local-vs-online safety fixes that were completed in this cycle.

Deployment targets:
- Frontend: Netlify
- Backend: Render Web Service (Docker)
- Database: TiDB Cloud or another managed MySQL-compatible database

This repository already contains the current deployment descriptors:

- `netlify.toml`
- `render.yaml`

If you want the shortest path, use `DEPLOY_MINIMAL_CHECKLIST.md` together with `DEPLOY_PLAN_B.md`.

## 1) Prepare Project Locally

1. In project root, create `.env` from template:

```bash
cp .env.example .env
```

2. Open `.env` and change `DB_PASSWORD` to a strong password.

3. Update frontend backend URL in:
- `frontend-prototype/runtime-config.js`

Set:

```js
window.__DATAFLOW_API_BASE__ = "http://YOUR_VM_PUBLIC_IP:8000";
```

Example:

```js
window.__DATAFLOW_API_BASE__ = "http://129.146.xx.xx:8000";
```

For this repository's current deployment path, use your Render backend URL instead, for example:

```js
window.__DATAFLOW_API_BASE__ = "https://transinfo.onrender.com";
```

## 2) Deploy Backend (Render)

1. Push this repository to GitHub.
2. In Render, create a new Web Service from the repository.
3. Use `render.yaml` or manually point Docker build to `backend/Dockerfile`.
4. Set environment variables:

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

CORS_ALLOW_ORIGINS=https://<your-netlify-domain>
```

5. Deploy and verify:

```bash
https://<your-render-domain>/docs
```

## 3) Deploy Frontend (Netlify)

1. Push this project to GitHub.
2. In Netlify:
- Create project from GitHub repo
- Set build command: (leave empty)
- Set output directory: `frontend-prototype`
3. Deploy.

Netlify gives a free URL like:
- `https://your-project.netlify.app`

## 4) Verify End-to-End

1. Open your Netlify URL.
2. Log in with the configured admin account.
3. Verify search, path mapping, export, and import cards.
4. Verify backend docs at `https://<your-render-domain>/docs`.

## 5) Version marker

This deployment batch is the v2.0.0 release.

Frontend version display is sourced from:

- `frontend-prototype/runtime-config.js`
- `frontend-prototype/app.js`
- `frontend-prototype/dev_proxy_server.py`
3. Try import status API from browser devtools/network.

## 5) Daily Operations

### Update backend code

```bash
docker compose up -d --build
```

### View logs

```bash
docker compose logs -f api
```

### Backup MySQL

```bash
docker exec dataflow-db sh -c 'mysqldump -uroot -p"$MYSQL_ROOT_PASSWORD" trans_fields_mapping' > backup.sql
```

### Restore MySQL

```bash
cat backup.sql | docker exec -i dataflow-db sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" trans_fields_mapping'
```

## Notes

- You do NOT need to buy a domain.
- You will still have URL access via:
  - Frontend: `*.pages.dev`
  - Backend: `http://VM_PUBLIC_IP:8000`
- If you later buy a domain, you only need to update DNS + `runtime-config.js`.
