import sqlite3

conn = sqlite3.connect('backend/data/trans_fields_mapping.db')
cur = conn.cursor()

print("=== rsdiobjt table columns ===")
cur.execute("PRAGMA table_info(rsdiobjt)")
columns = cur.fetchall()
for col in columns:
    print(f"  {col[1]} ({col[2]})")

conn.close()
