import pandas as pd
import mysql.connector
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[1]
EXCEL_PATH = Path(
    os.getenv(
        "BW_OBJECT_NAME_TABLE_DEFINITION_PATH",
        str(BASE_DIR / "Table-Template" / "table_definition_bw_object_name.xlsx"),
    )
)
TEMPLATE_REQUIRED = os.getenv("BW_OBJECT_NAME_TEMPLATE_REQUIRED", "false").strip().lower() == "true"
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "showlang")
DB_NAME = os.getenv("DB_NAME", "trans_fields_mapping")
TABLE_NAME = "bw_object_name"
DB_SSL_CA = os.getenv("DB_SSL_CA", "").strip()
DB_SSL_DISABLED = os.getenv("DB_SSL_DISABLED", "false").strip().lower() == "true"
DB_SSL_VERIFY_CERT = os.getenv("DB_SSL_VERIFY_CERT", "false").strip().lower() == "true"
DB_SSL_VERIFY_IDENTITY = os.getenv("DB_SSL_VERIFY_IDENTITY", "false").strip().lower() == "true"


def resolve_template_path() -> Path | None:
    if EXCEL_PATH.exists():
        return EXCEL_PATH
    alt = BASE_DIR / "Table Definition" / "table_definition_bw_object_name.xlsx"
    if alt.exists():
        return alt
    return None


def build_connect_kwargs() -> dict:
    connect_kwargs = {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }
    ssl_ca_path = Path(DB_SSL_CA) if DB_SSL_CA else None
    if DB_SSL_DISABLED:
        connect_kwargs["ssl_disabled"] = True
    elif ssl_ca_path and ssl_ca_path.exists():
        connect_kwargs["ssl_ca"] = DB_SSL_CA
        connect_kwargs["ssl_verify_cert"] = DB_SSL_VERIFY_CERT
        connect_kwargs["ssl_verify_identity"] = DB_SSL_VERIFY_IDENTITY
    return connect_kwargs


def build_columns(frame: pd.DataFrame):
    columns = []
    primary_keys = []
    seen_fields = set()

    # Legacy template compatibility: map NAME/OBJECT_NAME => NAME_EN.
    frame = frame.copy()
    frame["Field"] = frame["Field"].astype(str).str.strip().replace({"OBJECT_NAME": "NAME_EN", "NAME": "NAME_EN"})
    if not (frame["Field"].str.upper() == "NAME_DE").any():
        frame = pd.concat(
            [
                frame,
                pd.DataFrame(
                    [
                        {
                            "Field": "NAME_DE",
                            "Data type": "varchar",
                            "Len": 255,
                            "Decimals": 0,
                            "Field Text": "Object Name (DE)",
                            "KEY": "",
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    for _, row in frame.iterrows():
        field = str(row["Field"]).strip()
        if field.upper() == "DATASOURCE":
            continue
        if field in seen_fields:
            continue
        seen_fields.add(field)
        dtype = str(row["Data type"]).strip().lower()
        length = int(row["Len"])
        decimals = int(row["Decimals"]) if not pd.isna(row["Decimals"]) else 0
        comment = str(row["Field Text"]).replace("'", "''")

        if dtype == "varchar":
            col_type = f"VARCHAR({length})"
        elif dtype in {"char", "nchar"}:
            col_type = f"CHAR({length})"
        elif dtype in {"int", "integer"}:
            col_type = "INT"
        elif dtype in {"bigint"}:
            col_type = "BIGINT"
        elif dtype in {"decimal", "number", "numeric"}:
            col_type = f"DECIMAL({length},{decimals})"
        elif dtype == "date":
            col_type = "DATE"
        elif dtype in {"datetime", "timestamp"}:
            col_type = "DATETIME"
        else:
            col_type = f"VARCHAR({length})"

        is_key = str(row.get("KEY", "")).strip().upper() == "KEY"
        if TABLE_NAME == "bw_object_name" and field.upper() == "BW_OBJECT_TYPE":
            is_key = True
        null_sql = "NOT NULL" if is_key else "NULL"
        columns.append(f"  `{field}` {col_type} {null_sql} COMMENT '{comment}'")
        if is_key:
            primary_keys.append(f"`{field}`")

    pk_sql = f",\n  PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{DB_NAME}`.`{TABLE_NAME}` (\n"
        + ",\n".join(columns)
        + pk_sql
        + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    )

    return ddl


def main():
    resolved_path = resolve_template_path()
    if resolved_path is None:
        if TEMPLATE_REQUIRED:
            raise FileNotFoundError(f"BW object name table definition not found: {EXCEL_PATH}")
        print(f"[create_bw_object_name_table] Table definition file not found, skipping: {EXCEL_PATH}")
        return

    frame = pd.read_excel(str(resolved_path))
    ddl = build_columns(frame)

    conn = mysql.connector.connect(**build_connect_kwargs())
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4")
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()

    print(f"Created table: {DB_NAME}.{TABLE_NAME}")


if __name__ == "__main__":
    main()
