import argparse
from collections import OrderedDict

import mysql.connector as mc

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "showlang",
    "database": "trans_fields_mapping",
}


def norm(value: str, trim: bool) -> str:
    text = "" if value is None else str(value)
    return text.strip() if trim else text


def fetch_rows(cur, sources):
    placeholders = ", ".join(["%s"] * len(sources))
    sql = (
        "SELECT SOURCENAME, TARGETNAME "
        f"FROM rstran WHERE SOURCENAME IN ({placeholders}) "
        "ORDER BY SOURCENAME, TARGETNAME"
    )
    cur.execute(sql, tuple(sources))
    return cur.fetchall()


def trace(seed: str, max_steps: int, trim: bool):
    conn = mc.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        visited_sources = set()
        frontier = OrderedDict([(seed, None)])
        all_edges = []

        print(f"seed={repr(seed)} trim={trim}")
        for step in range(max_steps):
            pending = [s for s in frontier.keys() if s not in visited_sources]
            if not pending:
                print(f"step={step}: stop (no pending sources)")
                break

            rows = fetch_rows(cur, pending)
            next_frontier = OrderedDict()
            step_edges = []

            for src_raw, tgt_raw in rows:
                src = norm(src_raw, trim)
                tgt = norm(tgt_raw, trim)
                if not src or not tgt:
                    continue
                edge = (src, tgt)
                step_edges.append(edge)
                all_edges.append(edge)
                if tgt not in visited_sources:
                    next_frontier.setdefault(tgt, None)

            print(f"step={step}: pending={len(pending)} rows={len(rows)} unique_step_edges={len(set(step_edges))} next={len(next_frontier)}")
            if step_edges:
                preview = list(dict.fromkeys(step_edges))[:20]
                for src, tgt in preview:
                    print(f"  {src} -> {tgt}")

            visited_sources.update(pending)
            frontier = next_frontier

        unique_edges = list(dict.fromkeys(all_edges))
        print(f"\nsummary: total_unique_edges={len(unique_edges)}")
        for src, tgt in unique_edges[:120]:
            print(f"  {src} -> {tgt}")
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Step-by-step downstream trace debugger")
    parser.add_argument("--seed", required=True, help="Start SOURCENAME")
    parser.add_argument("--max-steps", type=int, default=20)
    parser.add_argument("--no-trim", action="store_true", help="Do not strip whitespace")
    args = parser.parse_args()

    trace(seed=args.seed, max_steps=args.max_steps, trim=not args.no_trim)


if __name__ == "__main__":
    main()
