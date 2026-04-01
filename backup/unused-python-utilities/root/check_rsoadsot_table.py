import sqlite3

conn = sqlite3.connect('backend/data/trans_fields_mapping.db')
cur = conn.cursor()

print("=== rsoadsot table info ===")
cur.execute("PRAGMA table_info(rsoadsot)")
columns = cur.fetchall()
for col in columns:
    print(f"  {col[1]} ({col[2]})")

print("\n=== rsoadsot indexes ===")
cur.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='rsoadsot'")
indexes = cur.fetchall()
if indexes:
    for idx in indexes:
        print(f"  {idx[0]}: {idx[1]}")
else:
    print("  No indexes found")

print("\n=== rsoadsot sample data ===")
cur.execute("SELECT * FROM rsoadsot LIMIT 3")
rows = cur.fetchall()
for row in rows:
    print(f"  {row}")

print("\n=== rsoadsot count ===")
cur.execute("SELECT COUNT(*) FROM rsoadsot")
print(f"  {cur.fetchone()[0]} rows")

conn.close()
