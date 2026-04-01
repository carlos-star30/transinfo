import sqlite3

conn = sqlite3.connect('backend/data/trans_fields_mapping.db')
cur = conn.cursor()

tables = ['rsoadsot', 'rsdiobjt', 'rstran_mapping_rule_full', 'rstran', 'rsdiobj']

for table in tables:
    print(f"\n=== {table} ===")
    cur.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?", (table,))
    indexes = cur.fetchall()
    if indexes:
        for idx in indexes:
            print(f"  {idx[0]}: {idx[2]}")
    else:
        print("  No indexes")

conn.close()
