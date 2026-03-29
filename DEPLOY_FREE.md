# Free Deployment Guide (No Custom Domain)

This guide gives you a long-term free setup with URL access.

Current stable release: v1.0

This v1.0 baseline includes the current searchable home page, flow tracing page, user login and admin management, datasource source-system tracing, and the latest lock/unlock safety rules.

Deployment targets:
- Frontend: Cloudflare Pages (free URL)
- Backend + MySQL: Oracle Cloud Always Free VM + Docker Compose

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

## 2) Deploy Backend (Oracle Free VM)

1. Create Ubuntu VM in Oracle Cloud Always Free.
2. Open inbound rules:
- TCP 22 (SSH)
- TCP 8000 (API)
3. SSH into VM and install Docker + Compose plugin.

4. Upload project to VM (git clone or scp).
5. In project root on VM:

```bash
cp .env.example .env
# edit .env with your DB_PASSWORD

docker compose up -d --build
```

6. Check health:

```bash
docker compose ps
curl http://127.0.0.1:8000/api/import-status
```

If API returns JSON, backend is ready.

## 3) Deploy Frontend (Cloudflare Pages)

1. Push this project to GitHub.
2. In Cloudflare Pages:
- Create project from GitHub repo
- Set build command: (leave empty)
- Set output directory: `frontend-prototype`
3. Deploy.

Cloudflare gives a free URL like:
- `https://your-project.pages.dev`

## 4) Verify End-to-End

1. Open your Pages URL.
2. Try search and flow trace.
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
