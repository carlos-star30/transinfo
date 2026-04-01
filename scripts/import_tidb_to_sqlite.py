import argparse
import sqlite3
from decimal import Decimal
from typing import Dict, List

import mysql.connector


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import TiDB table data into local SQLite tables.")
    parser.add_argument("--tidb-host", required=True)
    parser.add_argument("--tidb-port", type=int, default=4000)
    parser.add_argument("--tidb-user", required=True)
    parser.add_argument("--tidb-password", required=True)
    parser.add_argument("--tidb-database", required=True)
    parser.add_argument("--sqlite-path", default="backend/data/trans_fields_mapping.db")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--truncate-target", action="store_true", default=True)
    parser.add_argument("--no-truncate-target", action="store_false", dest="truncate_target")
    return parser.parse_args()


def mysql_quote(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def sqlite_quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def fetch_tidb_tables(conn: mysql.connector.MySQLConnection) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        return [str(row[0]) for row in cur.fetchall()]
    finally:
        cur.close()


def fetch_tidb_columns(conn: mysql.connector.MySQLConnection, table: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW COLUMNS FROM {mysql_quote(table)}")
        return [str(row[0]) for row in cur.fetchall()]
    finally:
        cur.close()


def fetch_sqlite_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        return [str(row[0]) for row in cur.fetchall()]
    finally:
        cur.close()


def fetch_sqlite_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({sqlite_quote(table)})")
        return [str(row[1]) for row in cur.fetchall()]
    finally:
        cur.close()


def sqlite_value(value):
    if isinstance(value, Decimal):
        return str(value)
    return value


def import_table(
    tidb_conn: mysql.connector.MySQLConnection,
    sqlite_conn: sqlite3.Connection,
    source_table: str,
    target_table: str,
    batch_size: int,
    truncate_target: bool,
) -> int:
    source_columns = fetch_tidb_columns(tidb_conn, source_table)
    target_columns = fetch_sqlite_columns(sqlite_conn, target_table)
    source_set = set(source_columns)
    common_columns = [col for col in target_columns if col in source_set]

    if not common_columns:
        print(f"[skip] {source_table}: no matching columns")
        return 0

    sqlite_cur = sqlite_conn.cursor()
    try:
        if truncate_target:
            sqlite_cur.execute(f"DELETE FROM {sqlite_quote(target_table)}")
            sqlite_conn.commit()

        source_col_sql = ", ".join(mysql_quote(col) for col in common_columns)
        target_col_sql = ", ".join(sqlite_quote(col) for col in common_columns)
        placeholders = ", ".join(["?"] * len(common_columns))
        select_sql = f"SELECT {source_col_sql} FROM {mysql_quote(source_table)}"
        insert_sql = f"INSERT INTO {sqlite_quote(target_table)} ({target_col_sql}) VALUES ({placeholders})"

        source_cur = tidb_conn.cursor(buffered=False)
        total = 0
        try:
            source_cur.execute(select_sql)
            while True:
                batch = source_cur.fetchmany(batch_size)
                if not batch:
                    break
                converted = [tuple(sqlite_value(item) for item in row) for row in batch]
                sqlite_cur.executemany(insert_sql, converted)
                total += len(batch)
                sqlite_conn.commit()
        finally:
            try:
                source_cur.close()
            except Exception:
                pass

        print(f"[ok] {source_table} -> {target_table}: {total} rows")
        return total
    finally:
        sqlite_cur.close()


def main() -> None:
    args = parse_args()

    tidb_conn = mysql.connector.connect(
        host=args.tidb_host,
        port=args.tidb_port,
        user=args.tidb_user,
        password=args.tidb_password,
        database=args.tidb_database,
        connection_timeout=60,
        read_timeout=3600,
        write_timeout=3600,
    )
    sqlite_conn = sqlite3.connect(args.sqlite_path)

    try:
        sqlite_conn.execute("PRAGMA foreign_keys = OFF")
        sqlite_conn.execute("PRAGMA journal_mode = WAL")
        sqlite_conn.execute("PRAGMA synchronous = NORMAL")

        source_tables = fetch_tidb_tables(tidb_conn)
        target_tables = fetch_sqlite_tables(sqlite_conn)
        target_map: Dict[str, str] = {name.lower(): name for name in target_tables}

        matched = []
        for src in source_tables:
            tgt = target_map.get(src.lower())
            if tgt:
                matched.append((src, tgt))

        print(f"matched tables: {len(matched)}")
        if not matched:
            print("No matching tables found between TiDB and SQLite.")
            return

        grand_total = 0
        for src, tgt in matched:
            grand_total += import_table(
                tidb_conn=tidb_conn,
                sqlite_conn=sqlite_conn,
                source_table=src,
                target_table=tgt,
                batch_size=args.batch_size,
                truncate_target=args.truncate_target,
            )

        print(f"done. total imported rows: {grand_total}")
    finally:
        try:
            sqlite_conn.close()
        except Exception:
            pass
        try:
            tidb_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
