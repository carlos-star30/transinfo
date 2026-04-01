import argparse
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, List, Sequence, Tuple

import mysql.connector


MYSQL_TO_SQLITE_TYPE = {
    "tinyint": "INTEGER",
    "smallint": "INTEGER",
    "mediumint": "INTEGER",
    "int": "INTEGER",
    "integer": "INTEGER",
    "bigint": "INTEGER",
    "float": "REAL",
    "double": "REAL",
    "decimal": "NUMERIC",
    "numeric": "NUMERIC",
    "char": "TEXT",
    "varchar": "TEXT",
    "text": "TEXT",
    "tinytext": "TEXT",
    "mediumtext": "TEXT",
    "longtext": "TEXT",
    "json": "TEXT",
    "date": "TEXT",
    "datetime": "TEXT",
    "timestamp": "TEXT",
    "time": "TEXT",
    "blob": "BLOB",
    "tinyblob": "BLOB",
    "mediumblob": "BLOB",
    "longblob": "BLOB",
    "binary": "BLOB",
    "varbinary": "BLOB",
    "bit": "INTEGER",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate MySQL database tables to a SQLite file.")
    parser.add_argument("--source-host", default="localhost")
    parser.add_argument("--source-port", type=int, default=3306)
    parser.add_argument("--source-user", default="root")
    parser.add_argument("--source-password", default="showlang")
    parser.add_argument("--source-database", default="trans_fields_mapping")
    parser.add_argument("--target-file", default="backend/data/trans_fields_mapping.db")
    parser.add_argument("--table", action="append", default=[], help="Repeat to migrate specific tables only")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--drop-existing", action="store_true")
    return parser.parse_args()


def mysql_tables(conn: Any) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        return [str(r[0]) for r in cur.fetchall()]
    finally:
        cur.close()


def mysql_columns(conn: Any, table_name: str) -> List[Tuple[str, str, str, str]]:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
        # Field, Type, Null, Key, Default, Extra
        return [(str(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in cur.fetchall()]
    finally:
        cur.close()


def sqlite_type(mysql_type: str) -> str:
    base = mysql_type.strip().lower().split("(", 1)[0]
    return MYSQL_TO_SQLITE_TYPE.get(base, "TEXT")


def build_create_table_sql(table_name: str, columns: Sequence[Tuple[str, str, str, str]]) -> str:
    col_defs: List[str] = []
    pk_cols: List[str] = []

    for name, raw_type, nullable, key in columns:
        sql_type = sqlite_type(raw_type)
        is_not_null = nullable.strip().upper() == "NO"
        if key.strip().upper() == "PRI":
            pk_cols.append(name)

        col_sql = f'"{name}" {sql_type}'
        if is_not_null:
            col_sql += " NOT NULL"
        col_defs.append(col_sql)

    if len(pk_cols) == 1:
        pk = pk_cols[0]
        rebuilt: List[str] = []
        for item in col_defs:
            if item.startswith(f'"{pk}" '):
                rebuilt.append(item + " PRIMARY KEY")
            else:
                rebuilt.append(item)
        col_defs = rebuilt
    elif len(pk_cols) > 1:
        pk_sql = ", ".join(f'"{c}"' for c in pk_cols)
        col_defs.append(f"PRIMARY KEY ({pk_sql})")

    body = ",\n  ".join(col_defs)
    return f'CREATE TABLE "{table_name}" (\n  {body}\n)'


def convert_row(row: Sequence[object]) -> Tuple[object, ...]:
    converted: List[object] = []
    for value in row:
        if isinstance(value, Decimal):
            converted.append(str(value))
        elif isinstance(value, datetime):
            converted.append(value.strftime("%Y-%m-%d %H:%M:%S"))
        elif isinstance(value, date):
            converted.append(value.strftime("%Y-%m-%d"))
        else:
            converted.append(value)
    return tuple(converted)


def migrate_table(
    source_conn: Any,
    target_conn: sqlite3.Connection,
    table_name: str,
    batch_size: int,
    drop_existing: bool,
) -> None:
    columns = mysql_columns(source_conn, table_name)
    if not columns:
        print(f"[skip] {table_name}: no columns")
        return

    column_names = [name for name, _, _, _ in columns]
    create_sql = build_create_table_sql(table_name, columns)

    cur_sqlite = target_conn.cursor()
    try:
        if drop_existing:
            cur_sqlite.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        cur_sqlite.execute(create_sql)
        target_conn.commit()
    finally:
        cur_sqlite.close()

    select_cols = ", ".join(f"`{name}`" for name in column_names)
    placeholders = ", ".join(["?"] * len(column_names))
    insert_sql = f'INSERT INTO "{table_name}" ({", ".join([f"\"{c}\"" for c in column_names])}) VALUES ({placeholders})'

    src_cur = source_conn.cursor(buffered=False)
    dst_cur = target_conn.cursor()
    copied = 0
    try:
        src_cur.execute(f"SELECT {select_cols} FROM `{table_name}`")
        while True:
            batch = src_cur.fetchmany(batch_size)
            if not batch:
                break
            converted = [convert_row(row) for row in batch]
            dst_cur.executemany(insert_sql, converted)
            target_conn.commit()
            copied += len(converted)
            print(f"  - {table_name}: {copied} rows copied")
    finally:
        src_cur.close()
        dst_cur.close()


def main() -> None:
    args = parse_args()

    target_file = Path(args.target_file).resolve()
    target_file.parent.mkdir(parents=True, exist_ok=True)

    source_conn = mysql.connector.connect(
        host=args.source_host,
        port=args.source_port,
        user=args.source_user,
        password=args.source_password,
        database=args.source_database,
    )
    target_conn = sqlite3.connect(str(target_file))

    try:
        target_conn.execute("PRAGMA journal_mode = WAL")
        target_conn.execute("PRAGMA synchronous = NORMAL")
        target_conn.execute("PRAGMA foreign_keys = OFF")

        all_tables = mysql_tables(source_conn)
        selected = args.table if args.table else all_tables

        missing = [name for name in selected if name not in all_tables]
        if missing:
            raise RuntimeError(f"Tables not found in source database: {missing}")

        print(f"Target SQLite file: {target_file}")
        print(f"Tables to migrate: {len(selected)}")

        for table_name in selected:
            print(f"[table] {table_name}")
            migrate_table(source_conn, target_conn, table_name, args.batch_size, args.drop_existing)

        target_conn.execute("PRAGMA foreign_keys = ON")
        target_conn.commit()
        print("Migration completed.")
    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()
