import argparse
import csv
import os
import tempfile
from typing import Iterable, Iterator, List

import mysql.connector
from mysql.connector import ProgrammingError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate all tables from local MySQL to TiDB.")
    parser.add_argument("--source-host", default="localhost")
    parser.add_argument("--source-port", type=int, default=3306)
    parser.add_argument("--source-user", default="root")
    parser.add_argument("--source-password", default="showlang")
    parser.add_argument("--source-database", default="trans_fields_mapping")
    parser.add_argument("--target-host", required=True)
    parser.add_argument("--target-port", type=int, default=4000)
    parser.add_argument("--target-user", required=True)
    parser.add_argument("--target-password", required=True)
    parser.add_argument("--target-database", required=True)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--load-chunk-rows", type=int, default=50000)
    parser.add_argument("--start-table")
    parser.add_argument("--skip-truncate", action="store_true")
    return parser.parse_args()


def fetch_tables(conn: mysql.connector.MySQLConnection) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        return [str(row[0]) for row in cur.fetchall()]
    finally:
        cur.close()


def fetch_columns(conn: mysql.connector.MySQLConnection, table_name: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
        return [str(row[0]) for row in cur.fetchall()]
    finally:
        cur.close()


def count_rows(conn: mysql.connector.MySQLConnection, table_name: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        return int(cur.fetchone()[0])
    finally:
        cur.close()


def batched_fetch(cursor: mysql.connector.cursor.MySQLCursor, batch_size: int) -> Iterable[list[tuple]]:
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield batch


def reset_target_table(target_conn: mysql.connector.MySQLConnection, table_name: str) -> None:
    cur = target_conn.cursor()
    try:
        print(f"  - truncating target table {table_name}")
        cur.execute(f"TRUNCATE TABLE `{table_name}`")
        target_conn.commit()
    except ProgrammingError as exc:
        if getattr(exc, "errno", None) != 1701:
            raise
        print(f"  - truncate blocked by foreign key on {table_name}, falling back to delete")
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute(f"DELETE FROM `{table_name}`")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        target_conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass


def write_table_dump_chunks(
    source_conn: mysql.connector.MySQLConnection,
    table_name: str,
    columns: List[str],
    batch_size: int,
    load_chunk_rows: int,
) -> Iterator[tuple[str, int]]:
    source_cur = source_conn.cursor(buffered=False)
    tmp = None
    writer = None
    chunk_rows = 0
    try:
        column_sql = ", ".join(f"`{column}`" for column in columns)
        source_cur.execute(f"SELECT {column_sql} FROM `{table_name}`")
        for batch in batched_fetch(source_cur, batch_size):
            for row in batch:
                if tmp is None:
                    tmp = tempfile.NamedTemporaryFile(
                        mode="w",
                        encoding="utf-8",
                        newline="",
                        suffix=f"_{table_name}.csv",
                        delete=False,
                    )
                    writer = csv.writer(
                        tmp,
                        delimiter=",",
                        quotechar='"',
                        quoting=csv.QUOTE_MINIMAL,
                        lineterminator="\n",
                    )
                    writer.writerow(columns)
                    chunk_rows = 0

                writer.writerow(["\\N" if value is None else value for value in row])
                chunk_rows += 1

                if chunk_rows >= load_chunk_rows:
                    tmp.close()
                    yield tmp.name, chunk_rows
                    tmp = None
                    writer = None

        if tmp is not None:
            tmp.close()
            yield tmp.name, chunk_rows
    finally:
        if tmp is not None and not tmp.closed:
            tmp.close()
        try:
            source_cur.close()
        except Exception:
            pass


def load_dump_into_target(
    target_conn: mysql.connector.MySQLConnection,
    table_name: str,
    columns: List[str],
    dump_path: str,
    truncate_before_load: bool,
) -> None:
    cur = target_conn.cursor()
    try:
        if truncate_before_load:
            cur.close()
            reset_target_table(target_conn, table_name)
            cur = target_conn.cursor()

        column_sql = ", ".join(f"`{column}`" for column in columns)
        load_sql = (
            f"LOAD DATA LOCAL INFILE %s INTO TABLE `{table_name}` "
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' "
            "LINES TERMINATED BY '\n' IGNORE 1 LINES "
            f"({column_sql})"
        )
        cur.execute(load_sql, (dump_path,))
        target_conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass


def migrate_table(
    source_conn: mysql.connector.MySQLConnection,
    target_conn: mysql.connector.MySQLConnection,
    table_name: str,
    batch_size: int,
    load_chunk_rows: int,
    skip_truncate: bool,
) -> None:
    source_columns = fetch_columns(source_conn, table_name)
    target_columns = fetch_columns(target_conn, table_name)
    if source_columns != target_columns:
        raise RuntimeError(f"Column mismatch for {table_name}: source={source_columns}, target={target_columns}")

    row_total = count_rows(source_conn, table_name)
    print(f"[table] {table_name}: source_rows={row_total}")

    try:
        if row_total == 0:
            if not skip_truncate:
                reset_target_table(target_conn, table_name)
            print(f"  - no rows to copy for {table_name}")
            return

        copied = 0
        chunk_index = 0
        truncate_pending = not skip_truncate
        for dump_path, dumped in write_table_dump_chunks(
            source_conn,
            table_name,
            source_columns,
            batch_size,
            load_chunk_rows,
        ):
            chunk_index += 1
            print(f"  - dumped chunk {chunk_index}: {dumped} rows")
            try:
                load_dump_into_target(target_conn, table_name, source_columns, dump_path, truncate_pending)
            finally:
                try:
                    if os.path.exists(dump_path):
                        os.remove(dump_path)
                except Exception:
                    pass
            truncate_pending = False
            copied += dumped
            print(f"  - loaded chunk {chunk_index}: {copied}/{row_total}")

        target_count = count_rows(target_conn, table_name)
        print(f"  - completed {table_name}: target_rows={target_count}")
        if target_count != row_total:
            raise RuntimeError(f"Row count mismatch for {table_name}: source={row_total}, target={target_count}")
    finally:
        pass


def main() -> None:
    args = parse_args()
    source_conn = mysql.connector.connect(
        host=args.source_host,
        port=args.source_port,
        user=args.source_user,
        password=args.source_password,
        database=args.source_database,
    )
    target_conn = mysql.connector.connect(
        host=args.target_host,
        port=args.target_port,
        user=args.target_user,
        password=args.target_password,
        database=args.target_database,
        allow_local_infile=True,
        connection_timeout=60,
        read_timeout=3600,
        write_timeout=3600,
    )

    try:
        source_tables = fetch_tables(source_conn)
        target_tables = set(fetch_tables(target_conn))
        missing_tables = [table for table in source_tables if table not in target_tables]
        if missing_tables:
            raise RuntimeError(f"Target database is missing tables: {missing_tables}")

        started = args.start_table is None
        for table_name in source_tables:
            if not started:
                if table_name != args.start_table:
                    continue
                started = True
            migrate_table(
                source_conn,
                target_conn,
                table_name,
                max(1, args.batch_size),
                max(1, args.load_chunk_rows),
                args.skip_truncate,
            )

        if args.start_table and not started:
            raise RuntimeError(f"Start table not found: {args.start_table}")
    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()