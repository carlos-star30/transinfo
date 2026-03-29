import argparse
import os
from pathlib import Path

import mysql.connector
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
TABLE_DEFINITION_PATH = BASE_DIR / "Table Definition" / "Table Definition.xlsx"
DEFAULT_METADATA_DIR = Path(os.getenv("METADATA_DIR", "/Volumes/Data/VS Code/Benz Tables"))

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "showlang")

DERIVED_COLUMNS = {
    "RSTRAN": [
        {
            "name": "SOURCE",
            "type": "VARCHAR(40)",
            "null_sql": "NULL",
            "comment": "Source object parsed from SOURCENAME before first space",
            "after": "SOURCENAME",
        },
        {
            "name": "SOURCESYS",
            "type": "VARCHAR(40)",
            "null_sql": "NULL",
            "comment": "Source system parsed from SOURCENAME after first space",
            "after": "SOURCE",
        },
    ]
}

INDEX_DEFINITIONS = {
    "RSTRAN": [
        ("idx_sourcename_objvers", ["SOURCENAME", "OBJVERS"]),
        ("idx_targetname_objvers", ["TARGETNAME", "OBJVERS"]),
        ("idx_source_objvers", ["SOURCE", "OBJVERS"]),
        ("idx_sourcesys_objvers", ["SOURCESYS", "OBJVERS"]),
    ],
    "RSTRANFIELD": [
        ("idx_tranid_objvers_ruleid_paramtype", ["TRANID", "OBJVERS", "RULEID", "PARAMTYPE"]),
    ],
    "RSTRANRULE": [
        ("idx_tranid_objvers_ruleid", ["TRANID", "OBJVERS", "RULEID"]),
        ("idx_objvers_groupid_tranid_ruleid", ["OBJVERS", "GROUPID", "TRANID", "RULEID"]),
    ],
}

COLUMN_TYPE_OVERRIDES = {
    ("RSOADSOT", "LANGU"): "CHAR(2)",
    ("RSDS", "CONVLANGU"): "CHAR(2)",
    ("RSDST", "LANGU"): "CHAR(2)",
}


def load_definition_rows() -> list[list[str]]:
    frame = pd.read_excel(TABLE_DEFINITION_PATH, sheet_name=0, header=0).fillna("")
    return [[str(value).strip() for value in row] for row in frame.values.tolist()]


def get_database_name(rows: list[list[str]]) -> str:
    for row in rows:
        if row and row[0] == "数据库" and len(row) > 1 and row[1]:
            return row[1]
    raise ValueError(f"Database name not found in {TABLE_DEFINITION_PATH}")


def get_table_name(rows: list[list[str]], table_order: int) -> str:
    label = f"表{table_order}"
    for row in rows:
        if row and row[0] == label and len(row) > 1 and row[1]:
            return row[1].strip()
    raise ValueError(f"{label} not found in {TABLE_DEFINITION_PATH}")


def find_metadata_file(metadata_dir: Path, table_code: str) -> Path:
    matches = sorted(metadata_dir.glob(f"*-{table_code}-*.xlsx"))
    if not matches:
        raise FileNotFoundError(f"No metadata file matched *-{table_code}-*.xlsx under {metadata_dir}")
    return matches[0]


def load_sap_metadata(path: Path) -> pd.DataFrame:
    frame = pd.read_excel(path, sheet_name=0, header=2)
    frame.columns = [str(column).strip() for column in frame.columns]
    return frame.fillna("")


def build_field_comment_map(field_texts: pd.DataFrame, fields: pd.DataFrame, data_element_texts: pd.DataFrame) -> dict[str, str]:
    comment_map = {}

    if not field_texts.empty:
        for _, row in field_texts.iterrows():
            field_name = str(row["FIELDNAME"]).strip()
            text = str(row["DDTEXT"]).strip()
            if field_name and text:
                comment_map[field_name] = text

    if data_element_texts.empty:
        return comment_map

    data_element_comment_map = {
        str(row["ROLLNAME"]).strip(): str(row["DDTEXT"]).strip()
        for _, row in data_element_texts.iterrows()
        if str(row["ROLLNAME"]).strip() and str(row["DDTEXT"]).strip()
    }

    for _, row in fields.iterrows():
        field_name = str(row["FIELDNAME"]).strip()
        rollname = str(row.get("ROLLNAME", "")).strip()
        if field_name and field_name not in comment_map and rollname in data_element_comment_map:
            comment_map[field_name] = data_element_comment_map[rollname]

    return comment_map


def mysql_type_for(table_name: str, row: pd.Series) -> str:
    field_name = str(row["FIELDNAME"]).strip()
    override = COLUMN_TYPE_OVERRIDES.get((table_name, field_name))
    if override:
        return override

    datatype = str(row["DATATYPE"]).strip().upper()
    length = int(row["LENG"] or 0)
    decimals = int(row["DECIMALS"] or 0)

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


def column_sql_from_parts(field_name: str, data_type_sql: str, null_sql: str, comment: str) -> str:
    safe_comment = comment.replace("'", "''")
    return f"  `{field_name}` {data_type_sql} {null_sql} COMMENT '{safe_comment}'"


def get_defined_index_names(table_name: str, field_names: set[str]) -> list[tuple[str, list[str]]]:
    definitions = []
    for index_name, columns in INDEX_DEFINITIONS.get(table_name, []):
        if set(columns).issubset(field_names):
            definitions.append((index_name, columns))
    return definitions


def add_missing_columns(cursor, table_name: str, database_name: str) -> None:
    for spec in DERIVED_COLUMNS.get(table_name, []):
        cursor.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
            """,
            (database_name, table_name, spec["name"]),
        )
        exists = int(cursor.fetchone()[0]) > 0
        if exists:
            continue

        safe_comment = spec["comment"].replace("'", "''")
        cursor.execute(
            f"ALTER TABLE `{table_name}` ADD COLUMN `{spec['name']}` {spec['type']} {spec['null_sql']} COMMENT '{safe_comment}' AFTER `{spec['after']}`"
        )


def ensure_indexes(cursor, table_name: str, field_names: set[str]) -> None:
    cursor.execute(f"SHOW INDEX FROM `{table_name}`")
    existing_index_names = {str(row[2]) for row in cursor.fetchall()}

    for index_name, columns in get_defined_index_names(table_name, field_names):
        if index_name in existing_index_names:
            continue
        column_sql = ", ".join(f"`{column}`" for column in columns)
        cursor.execute(f"ALTER TABLE `{table_name}` ADD INDEX `{index_name}` ({column_sql})")


def build_table_ddl(table_name: str, table_text: str, fields: pd.DataFrame, field_comment_map: dict[str, str]) -> str:
    ordered_fields = fields.sort_values(by=["POSITION", "FIELDNAME"], kind="stable")
    column_sql = []
    primary_keys = []
    field_names = {str(value).strip() for value in ordered_fields["FIELDNAME"].tolist()}

    for _, row in ordered_fields.iterrows():
        field_name = str(row["FIELDNAME"]).strip()
        if not field_name:
            continue
        data_type_sql = mysql_type_for(table_name, row)
        not_null = str(row.get("NOTNULL", "")).strip().upper() == "X" or str(row.get("KEYFLAG", "")).strip().upper() == "X"
        comment = field_comment_map.get(field_name) or field_name
        null_sql = "NOT NULL" if not_null else "NULL"
        column_sql.append(column_sql_from_parts(field_name, data_type_sql, null_sql, comment))
        if str(row.get("KEYFLAG", "")).strip().upper() == "X":
            primary_keys.append(f"`{field_name}`")

        if table_name == "RSTRAN" and field_name == "SOURCENAME":
            for spec in DERIVED_COLUMNS.get(table_name, []):
                field_names.add(spec["name"])
                column_sql.append(column_sql_from_parts(spec["name"], spec["type"], spec["null_sql"], spec["comment"]))

    extra_indexes = []
    for index_name, columns in get_defined_index_names(table_name, field_names):
        column_list = ", ".join(f"`{column}`" for column in columns)
        extra_indexes.append(f"  KEY `{index_name}` ({column_list})")

    body = column_sql[:]
    if primary_keys:
        body.append(f"  PRIMARY KEY ({', '.join(primary_keys)})")
    body.extend(extra_indexes)

    table_comment = (table_text or table_name).replace("'", "''")
    return (
        f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
        + ",\n".join(body)
        + f"\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{table_comment}';"
    )


def sync_existing_table(cursor, database_name: str, table_name: str, table_text: str, fields: pd.DataFrame, field_comment_map: dict[str, str]) -> None:
    ordered_fields = fields.sort_values(by=["POSITION", "FIELDNAME"], kind="stable")
    modifications = []

    for _, row in ordered_fields.iterrows():
        field_name = str(row["FIELDNAME"]).strip()
        if not field_name:
            continue
        data_type_sql = mysql_type_for(table_name, row)
        not_null = str(row.get("NOTNULL", "")).strip().upper() == "X" or str(row.get("KEYFLAG", "")).strip().upper() == "X"
        comment = field_comment_map.get(field_name) or field_name
        null_sql = "NOT NULL" if not_null else "NULL"
        safe_comment = comment.replace("'", "''")
        modifications.append(f"MODIFY COLUMN `{field_name}` {data_type_sql} {null_sql} COMMENT '{safe_comment}'")

    if modifications:
        cursor.execute(f"ALTER TABLE `{table_name}`\n  " + ",\n  ".join(modifications))

    add_missing_columns(cursor, table_name, database_name)

    existing_field_names = {str(value).strip() for value in ordered_fields["FIELDNAME"].tolist()}
    existing_field_names.update(spec["name"] for spec in DERIVED_COLUMNS.get(table_name, []))
    ensure_indexes(cursor, table_name, existing_field_names)

    table_comment = (table_text or table_name).replace("'", "''")
    cursor.execute(f"ALTER TABLE `{table_name}` COMMENT = '{table_comment}'")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a MySQL table from Table Definition and SAP DD metadata.")
    parser.add_argument("--table-order", type=int, default=1, help="Table order from Table Definition.xlsx, for example 1 => 表1")
    parser.add_argument("--metadata-dir", default=str(DEFAULT_METADATA_DIR), help="Directory containing DD02T/DD03L/DD03T Excel files")
    args = parser.parse_args()

    rows = load_definition_rows()
    database_name = get_database_name(rows)
    table_name = get_table_name(rows, args.table_order)
    metadata_dir = Path(args.metadata_dir)

    dd02t = load_sap_metadata(find_metadata_file(metadata_dir, "DD02T"))
    dd03l = load_sap_metadata(find_metadata_file(metadata_dir, "DD03L"))
    dd03t = load_sap_metadata(find_metadata_file(metadata_dir, "DD03T"))
    dd04t = load_sap_metadata(find_metadata_file(metadata_dir, "DD04T"))

    table_text_row = dd02t[
        (dd02t["TABNAME"].astype(str).str.strip() == table_name)
        & (dd02t["DDLANGUAGE"].astype(str).str.strip() == "EN")
    ]
    table_text = ""
    if not table_text_row.empty:
        table_text = str(table_text_row.iloc[0]["DDTEXT"]).strip()

    fields = dd03l[dd03l["TABNAME"].astype(str).str.strip() == table_name].copy()
    if fields.empty:
        raise ValueError(f"No DD03L field definitions found for {table_name}")

    field_texts = dd03t[
        (dd03t["TABNAME"].astype(str).str.strip() == table_name)
        & (dd03t["DDLANGUAGE"].astype(str).str.strip() == "EN")
    ].copy()

    data_element_texts = dd04t[dd04t["DDLANGUAGE"].astype(str).str.strip() == "EN"].copy()
    field_comment_map = build_field_comment_map(field_texts, fields, data_element_texts)

    ddl = build_table_ddl(table_name, table_text, fields, field_comment_map)

    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}` DEFAULT CHARACTER SET utf8mb4")
    cur.execute(f"USE `{database_name}`")
    cur.execute(ddl)
    sync_existing_table(cur, database_name, table_name, table_text, fields, field_comment_map)
    conn.commit()
    cur.close()
    conn.close()

    print(f"Created table: {database_name}.{table_name}")
    print(f"Table comment: {table_text or table_name}")
    print("Primary key fields:", ", ".join(fields[fields['KEYFLAG'].astype(str).str.strip() == 'X']['FIELDNAME'].astype(str).tolist()))
    print(f"Column count: {len(fields)}")


if __name__ == "__main__":
    main()