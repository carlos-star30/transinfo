from __future__ import annotations

import re
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DOC_PATH = BASE_DIR / "docs" / "trans_fields_mapping_table_definitions.md"
DB_PATH = BASE_DIR / "backend" / "data" / "trans_fields_mapping.db"


def extract_sql_blocks(markdown_text: str) -> list[str]:
    return re.findall(r"```sql\s*(.*?)```", markdown_text, flags=re.DOTALL | re.IGNORECASE)


def split_body_lines(body: str) -> list[str]:
    return [line.strip().rstrip(",") for line in body.strip().splitlines() if line.strip()]


def sqlite_type(mysql_type: str) -> str:
    normalized = re.sub(r"\s+", " ", mysql_type.strip().lower())
    normalized = normalized.replace("unsigned", "").strip()
    base = normalized.split("(", 1)[0].strip()
    if base in {"bigint", "int", "integer", "smallint", "tinyint", "mediumint"}:
        return "INTEGER"
    if base in {"decimal", "numeric", "double", "float", "real"}:
        return "NUMERIC"
    if base in {"datetime", "timestamp", "date", "time", "year"}:
        return "TEXT"
    if base in {"char", "varchar", "text", "tinytext", "mediumtext", "longtext", "json", "enum", "set"}:
        return "TEXT"
    if base in {"blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary"}:
        return "BLOB"
    return "TEXT"


def convert_default(default_value: str) -> str:
    raw = default_value.strip()
    upper = raw.upper()
    if upper == "NULL":
        return "NULL"
    if upper == "CURRENT_TIMESTAMP":
        return "CURRENT_TIMESTAMP"
    return raw


def parse_column(line: str) -> dict[str, object]:
    match = re.match(r"`([^`]+)`\s+(.+)$", line)
    if not match:
        raise ValueError(f"Unsupported column line: {line}")

    name = match.group(1)
    remainder = match.group(2)
    type_match = re.match(r"([A-Za-z]+(?:\s+unsigned)?(?:\([^)]*\))?)\s*(.*)$", remainder, flags=re.IGNORECASE)
    if not type_match:
        raise ValueError(f"Cannot parse column type: {line}")

    mysql_col_type = type_match.group(1).strip()
    tail = type_match.group(2).strip()
    tail = re.sub(r"\s+COMMENT\s+'(?:[^']|'{2})*'\s*$", "", tail, flags=re.IGNORECASE)
    tail = re.sub(r"\s+ON\s+UPDATE\s+CURRENT_TIMESTAMP\s*", " ", tail, flags=re.IGNORECASE)
    upper_tail = tail.upper()

    not_null = "NOT NULL" in upper_tail
    auto_increment = "AUTO_INCREMENT" in upper_tail
    default_match = re.search(r"DEFAULT\s+('(?:[^']|''*)*'|[^\s,]+)", tail, flags=re.IGNORECASE)
    default_sql = convert_default(default_match.group(1)) if default_match else None

    return {
        "name": name,
        "sqlite_type": sqlite_type(mysql_col_type),
        "not_null": not_null,
        "default_sql": default_sql,
        "auto_increment": auto_increment,
    }


def convert_create_statement(create_sql: str) -> tuple[str, list[str], str]:
    statement = create_sql.strip().rstrip(";")
    match = re.match(
        r"CREATE TABLE\s+`([^`]+)`\s*\((.*)\)\s*ENGINE=.*$",
        statement,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not match:
        raise ValueError("Unsupported CREATE TABLE statement")

    table_name = match.group(1)
    body = match.group(2)
    lines = split_body_lines(body)

    columns: list[dict[str, object]] = []
    pk_columns: list[str] = []
    indexes: list[tuple[bool, str, str]] = []
    table_constraints: list[str] = []

    for line in lines:
        if line.startswith("`"):
            columns.append(parse_column(line))
            continue
        if line.upper().startswith("PRIMARY KEY"):
            pk_columns = re.findall(r"`([^`]+)`", line)
            continue
        if line.upper().startswith("UNIQUE KEY"):
            parts = re.findall(r"`([^`]+)`", line)
            if parts:
                indexes.append((True, parts[0], ", ".join(f'"{name}"' for name in parts[1:])))
            continue
        if line.upper().startswith("KEY "):
            parts = re.findall(r"`([^`]+)`", line)
            if parts:
                indexes.append((False, parts[0], ", ".join(f'"{name}"' for name in parts[1:])))
            continue
        if line.upper().startswith("CONSTRAINT ") or line.upper().startswith("FOREIGN KEY"):
            converted = re.sub(r"`([^`]+)`", r'"\1"', line)
            converted = re.sub(r"ON DELETE CASCADE", "ON DELETE CASCADE", converted, flags=re.IGNORECASE)
            table_constraints.append(converted)
            continue

    autoincrement_pk = None
    if len(pk_columns) == 1:
        candidate = pk_columns[0]
        for column in columns:
            if column["name"] == candidate and column["auto_increment"] and column["sqlite_type"] == "INTEGER":
                autoincrement_pk = candidate
                break

    column_sql_lines: list[str] = []
    for column in columns:
        name = str(column["name"])
        sqlite_col_type = str(column["sqlite_type"])
        if autoincrement_pk == name:
            column_sql_lines.append(f'"{name}" INTEGER PRIMARY KEY AUTOINCREMENT')
            continue

        parts = [f'"{name}" {sqlite_col_type}']
        if bool(column["not_null"]):
            parts.append("NOT NULL")
        if column["default_sql"] is not None:
            parts.append(f'DEFAULT {column["default_sql"]}')
        column_sql_lines.append(" ".join(parts))

    if pk_columns and autoincrement_pk is None:
        column_sql_lines.append("PRIMARY KEY (" + ", ".join(f'"{name}"' for name in pk_columns) + ")")
    column_sql_lines.extend(table_constraints)

    create_table_sql = (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  '
        + ",\n  ".join(column_sql_lines)
        + "\n)"
    )

    index_sql: list[str] = []
    for is_unique, index_name, columns_sql in indexes:
        unique_sql = "UNIQUE " if is_unique else ""
        index_sql.append(
            f'CREATE {unique_sql}INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ({columns_sql})'
        )

    return table_name, index_sql, create_table_sql


def main() -> None:
    markdown_text = DOC_PATH.read_text(encoding="utf-8")
    sql_blocks = extract_sql_blocks(markdown_text)
    if not sql_blocks:
        raise RuntimeError(f"No SQL blocks found in {DOC_PATH}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        created_tables: list[str] = []
        for block in sql_blocks:
            table_name, index_sql_list, create_table_sql = convert_create_statement(block)
            conn.execute(create_table_sql)
            for sql in index_sql_list:
                conn.execute(sql)
            created_tables.append(table_name)
        conn.commit()
    finally:
        conn.close()

    print(f"Created or verified {len(sql_blocks)} tables in {DB_PATH}")


if __name__ == "__main__":
    main()