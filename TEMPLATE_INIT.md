# Demo Project Reference Checklist

Use this checklist as a reference when creating a new app from the starter template.

## 1. Copy From the Starter Template

Example:

```bash
mkdir my-new-app
cp -R /path/to/project-template/. /path/to/my-new-app/
```

Then open the new project directory in VS Code.

## 2. Rename Core Identity Values First

For this demo project, the key identity values are:

- `示例业务应用`
- `trans_fields_mapping`
- `trans_fields_mapping_session`
- `example-business-app-api`

These are the first values you normally replace in a fresh app.

## 3. Update These Files First

- `README.md`
- `.env.example`
- `frontend-prototype/runtime-config.js`
- `frontend-prototype/index.html`
- `frontend-prototype/styles.css`
- `render.yaml`
- deployment markdown files in project root and `docs/`

## 4. UI Sizing Rule

If the new app needs different button, input, table, or spacing sizes, update the tokens at the top of `frontend-prototype/styles.css` first:

- `--control-*`
- `--space-*`
- `--table-*`

Do not start by editing individual components one by one.

## 5. Feature Review

Decide early whether the new app still needs:

- login and user management UI
- Excel import flow
- flow graph pages
- mock data utilities
- local proxy helper
- table creation scripts

Remove unused features early so the new project does not inherit dead code.

## 6. First Validation Pass

Validate that the new project can:

- open the frontend homepage
- start the backend without import errors
- connect to the target database
- pass the health check script
- complete the core business workflow

- API routes in `backend/import_status_api.py`
- Python dependencies in `backend/requirements.txt`
- startup behavior in `backend/entrypoint.sh`
- database name and credentials in `.env`
- table creation scripts in `scripts/`
- spreadsheet templates in `Table-Template/`

## 6. Search For Old Product Strings

Run a global search for these kinds of values and replace them:

- `Dataflow Digram`
- `数据流查看工具`
- `dataflow_digram`
- `trans_fields_mapping`
- `trans_fields_mapping_session`
- old API URLs
- old admin defaults

## 7. Clean Environment and Data

Before first commit of the new project:

- create a fresh `.env` from `.env.example`
- verify no production secrets exist in the repo
- remove old logs, exports, test spreadsheets, and generated files
- confirm backup directories are not present

## 8. First Validation Pass

Validate the new project can start:

- frontend page opens
- backend starts without import errors
- database connects with the new settings
- health check script passes
- login and core workflow match the new business purpose

## 9. Recommended Next Improvement

After the first successful new project, create a second-generation template by extracting:

- design tokens for colors, typography, and more detailed component states
- reusable base component classes
- a smaller backend starter with clearer module boundaries

That will make later projects lighter and easier to maintain.
