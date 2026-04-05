# 示例业务应用

This directory is an example app created from the reusable full-stack starter.

## Included

- `frontend-prototype/`: reusable UI shell, styles, pages, runtime API config
- `backend/`: Python API service skeleton and import/status endpoints
- `scripts/`: table creation, health check, local run and deploy helpers
- `Table-Template/`: Excel table definition templates used by startup scripts
- `docs/`: deployment guides and environment checklists
- root deploy files: `Dockerfile`, `docker-compose.yml`, `netlify.toml`, `render.yaml`, `.env.example`

## This Demo Shows

1. A new project directory created from the reusable starter template.
2. Core identity values renamed to a concrete example app.
3. Frontend title, database defaults, and deploy service name updated.
4. The remaining initialization checklist kept in `TEMPLATE_INIT.md`.

## Notes

- This example was initialized from the reusable starter template and can be used as a reference for future app initialization.
- Common UI sizes are centralized at the top of `frontend-prototype/styles.css`.
- If you need to change button height, input height, row height, or common spacing, update the `--control-*`, `--space-*`, and `--table-*` tokens first.
- ABAP 转 SQL 当前已固化的规则说明见 `docs/abap-to-sql-rules.md`。

## Local Startup

- Use `LOCAL_STARTUP.md` for the fixed local startup flow (frontend `8088` + backend `8000`) and troubleshooting steps.
- Use `SQLITE_LOCAL_STARTUP.md` for the current Windows SQLite local startup path.
- Use `docs/path-selection-logic-and-table-relations.md` when troubleshooting path search, mapping assembly, diagnostics, or export behavior.

## Environment Commands

- Say `启动本地项目` when you want local frontend + local backend + local SQLite.
- Say `检查线上项目` when you want read-only checks against the deployed environment.
- Do not use `启动项目` alone, because it is ambiguous for this repository.

## Production Deployment

- Current recommended production path: Netlify + Render + TiDB.
- Canonical deployment guide: `DEPLOY_PLAN_B.md`.
- Render environment checklist: `RENDER_ENV_PRODUCTION.md`.
- Release note for current version: `RELEASE_v2.0.1.md`.

