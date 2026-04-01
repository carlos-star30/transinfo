import sqlite3

conn = sqlite3.connect('backend/data/trans_fields_mapping.db')
cur = conn.cursor()

print("=== All indexes ===")
cur.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index' ORDER BY tbl_name, name")
indexes = cur.fetchall()
for idx in indexes:
    print(f"{idx[1]}.{idx[0]}: {idx[2]}")

conn.close()
