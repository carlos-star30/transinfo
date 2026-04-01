# Next Deployment Quick Notes

This file records the minimum information needed next time so deployment does not need to be re-discovered.

## Current preferred scheme

- Frontend: Netlify
- Backend: Render
- Database: TiDB Cloud / MySQL-compatible managed DB

## Current production endpoints

- Frontend: https://transinfo.netlify.app
- Backend: https://transinfo.onrender.com

## Files to check before release

1. `frontend-prototype/runtime-config.js`
2. `frontend-prototype/index.html`
3. `render.yaml`
4. `RENDER_ENV_PRODUCTION.md`
5. `RELEASE_v2.0.0.md`

## Usually only these items need updating

1. Frontend app version and asset cache suffix.
2. Release note file for the new version.
3. Render environment variables only if domain, DB, or auth policy changed.
4. `runtime-config.js` only if backend domain changed.

## Current Render live choices to remember

1. `AUTH_COOKIE_NAME=transinfo_session`
2. `CORS_ALLOW_ORIGINS=https://transinfo.netlify.app`
3. `DB_PORT=4000`
4. `DB_SSL_CA=/etc/ssl/certs/ca-certificates.crt`
5. `DB_NAME` must match the actual TiDB schema; current live environment may use `test`

## Usually not needed again

1. Re-choosing deployment platform.
2. Rebuilding deployment topology docs from scratch.
3. Reconfirming whether Netlify or Render is the active production route.