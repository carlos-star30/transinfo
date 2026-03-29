# 示例业务应用

This directory is an example app created from the reusable full-stack starter.

## Included

- `frontend-prototype/`: reusable UI shell, styles, pages, runtime API config
- `backend/`: Python API service skeleton and import/status endpoints
- `scripts/`: table creation, health check, local run and deploy helpers
- `Table-Template/`: Excel table definition templates used by startup scripts
- `docs/`: deployment guides for Netlify + Railway + TiDB
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

## Local Startup

- Use `LOCAL_STARTUP.md` for the fixed local startup flow (frontend `8088` + backend `8000`) and troubleshooting steps.
