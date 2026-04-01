import sqlite3

conn = sqlite3.connect('backend/data/trans_fields_mapping.db')
cur = conn.cursor()

print("=== rsoadsot indexes ===")
cur.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='rsoadsot'")
indexes = cur.fetchall()
if indexes:
    for idx in indexes:
        print(f"  {idx[0]}: {idx[1]}")
else:
    print("  No indexes found")

print("\n=== rstran indexes ===")
cur.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='rstran'")
indexes = cur.fetchall()
for idx in indexes:
    print(f"  {idx[0]}: {idx[1]}")

conn.close()
