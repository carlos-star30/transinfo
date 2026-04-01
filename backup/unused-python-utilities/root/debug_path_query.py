import sqlite3

conn = sqlite3.connect('backend/data/trans_fields_mapping.db')
cur = conn.cursor()

# Source and Target from the screenshot
source = "0CO_OM_OPA_6"
target = "CEOPA6BP1"

print(f"=== Checking path from {source} to {target} ===\n")

# Check RSTRAN records matching source
print(f"=== RSTRAN records with SOURCENAME = '{source}' ===")
cur.execute("""
    SELECT TRANID, SOURCENAME, TARGETNAME, SOURCESYS, OBJVERS
    FROM RSTRAN
    WHERE UPPER(TRIM(SOURCENAME)) = UPPER(?)
    ORDER BY TRANID
""", (source,))

rows = cur.fetchall()
print(f"Found {len(rows)} records:")
for row in rows:
    print(f"  {row}")

# Check RSTRAN records matching target
print(f"\n=== RSTRAN records with TARGETNAME = '{target}' ===")
cur.execute("""
    SELECT TRANID, SOURCENAME, TARGETNAME, SOURCESYS, OBJVERS
    FROM RSTRAN
    WHERE UPPER(TRIM(TARGETNAME)) = UPPER(?)
    ORDER BY TRANID
""", (target,))

rows = cur.fetchall()
print(f"Found {len(rows)} records:")
for row in rows:
    print(f"  {row}")

# Check if there's a path through intermediate objects
print(f"\n=== Checking 2-hop paths ===")
cur.execute("""
    SELECT DISTINCT r1.TARGETNAME as intermediate, 
           (SELECT COUNT(*) FROM RSTRAN r2 WHERE UPPER(TRIM(r2.SOURCENAME)) = UPPER(TRIM(r1.TARGETNAME)) AND UPPER(TRIM(r2.TARGETNAME)) = UPPER(?)) as connections_to_target
    FROM RSTRAN r1
    WHERE UPPER(TRIM(r1.SOURCENAME)) = UPPER(?)
    HAVING connections_to_target > 0
""", (target, source))

rows = cur.fetchall()
if rows:
    print(f"Found {len(rows)} intermediate objects:")
    for row in rows:
        print(f"  {row[0]} -> {target} ({row[1]} connections)")
else:
    print("No 2-hop paths found")

conn.close()
