import os
from pathlib import Path

import mysql.connector
import pandas as pd


METADATA_PATH = Path(os.getenv("DD03L_METADATA_PATH", "/app/Excel Data/P4C-DD03L-Table Fields-RS_DD-0317.xlsx"))
METADATA_REQUIRED = os.getenv("DD03L_METADATA_REQUIRED", "false").strip().lower() == "true"
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "showlang")
DB_NAME = os.getenv("DB_NAME", "trans_fields_mapping")
TABLE_NAME = "dd03l"


def mysql_type_for(row: pd.Series) -> str:
    datatype = str(row.get("DATATYPE") or "").strip().upper()
    length = int(row.get("LENG") or 0)
    decimals = int(row.get("DECIMALS") or 0)

    if datatype in {"CHAR", "NUMC", "CLNT", "LANG", "CUKY", "UNIT", "DATS", "TIMS"}:
        return f"CHAR({max(length, 1)})"
    if datatype == "INT1":
        return "TINYINT UNSIGNED"
    if datatype == "INT2":
        return "SMALLINT"
    if datatype in {"INT4", "INT"}:
        return "INT"
    if datatype == "INT8":
        return "BIGINT"
    if datatype in {"DEC", "CURR", "QUAN", "DECIMAL", "NUMERIC"}:
        precision = max(length, decimals + 1, 1)
        return f"DECIMAL({precision},{decimals})"
    if datatype in {"FLTP", "FLOAT"}:
        return "DOUBLE"
    if datatype in {"STRG", "STRING", "SSTRING"}:
        return "TEXT"
    if datatype in {"RAW", "LRAW"}:
        return f"VARBINARY({max(length, 1)})"
    return f"VARCHAR({max(length, 1)})"


def get_observed_char_lengths(frame: pd.DataFrame) -> dict[str, int]:
    observed: dict[str, int] = {}
    for column in frame.columns:
        column_name = str(column or "").strip()
        if not column_name:
            continue
        observed[column_name] = int(frame[column_name].astype(str).str.len().max() or 0)
    return observed


def mysql_storage_type_for(row: pd.Series, observed_lengths: dict[str, int]) -> str:
    field_name = str(row.get("FIELDNAME") or "").strip()
    base_type = mysql_type_for(row)
    if not base_type.startswith("CHAR("):
        return base_type
    metadata_length = int(base_type[5:-1])
    observed_length = int(observed_lengths.get(field_name, 0) or 0)
    return f"CHAR({max(metadata_length, observed_length, 1)})"


def build_ddl(frame: pd.DataFrame) -> str:
    observed_lengths = get_observed_char_lengths(frame)
    fields = frame[frame["TABNAME"].astype(str).str.strip() == "DD03L"].copy()
    if fields.empty:
        raise ValueError("No DD03L field definitions found in metadata file")

    ordered_fields = fields.sort_values(by=["POSITION", "FIELDNAME"], kind="stable")
    columns = []
    primary_keys = []

    for _, row in ordered_fields.iterrows():
        field_name = str(row.get("FIELDNAME") or "").strip()
        if not field_name:
            continue
        data_type_sql = mysql_storage_type_for(row, observed_lengths)
        is_key = str(row.get("KEYFLAG") or "").strip().upper() == "X"
        is_not_null = is_key or str(row.get("NOTNULL") or "").strip().upper() == "X"
        null_sql = "NOT NULL" if is_not_null else "NULL"
        columns.append(f"  `{field_name}` {data_type_sql} {null_sql} COMMENT '{field_name}'")
        if is_key:
            primary_keys.append(f"`{field_name}`")

    pk_sql = f",\n  PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
    return (
        f"CREATE TABLE IF NOT EXISTS `{DB_NAME}`.`{TABLE_NAME}` (\n"
        + ",\n".join(columns)
        + pk_sql
        + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DD03L table field metadata';"
    )


def main() -> None:
    if not METADATA_PATH.exists():
        if METADATA_REQUIRED:
            raise FileNotFoundError(f"Metadata file not found: {METADATA_PATH}")
        print(f"[create_dd03l_table] Metadata file not found, skipping: {METADATA_PATH}")
        return

    frame = pd.read_excel(METADATA_PATH, sheet_name=0, header=2).fillna("")
    frame.columns = [str(column).strip() for column in frame.columns]
    ddl = build_ddl(frame)

    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4")
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()

    print(f"Created table: {DB_NAME}.{TABLE_NAME}")


if __name__ == "__main__":
    main()