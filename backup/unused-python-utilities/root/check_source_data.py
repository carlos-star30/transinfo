import sqlite3

conn = sqlite3.connect('backend/data/trans_fields_mapping.db')
cur = conn.cursor()

print("=== Checking if 0CO_OM_OPA_6 exists in RSTRAN ===")
cur.execute("""
    SELECT DISTINCT SOURCENAME 
    FROM RSTRAN 
    WHERE UPPER(TRIM(SOURCENAME)) LIKE '%OPA%'
    LIMIT 20
""")
rows = cur.fetchall()
print(f"Source objects containing 'OPA': {len(rows)}")
for r in rows:
    print(f"  {r[0]}")

print("\n=== Checking all TRANIDs for 0CO_OM_OPA_6 ===")
cur.execute("""
    SELECT TRANID, SOURCENAME, TARGETNAME 
    FROM RSTRAN 
    WHERE UPPER(TRIM(SOURCENAME)) = '0CO_OM_OPA_6'
""")
rows = cur.fetchall()
print(f"Found {len(rows)} records")
for r in rows:
    print(f"  {r}")

print("\n=== Checking RSDS objects for 0CO_OM_OPA_6 ===")
cur.execute("""
    SELECT DISTINCT SOURCE, SOURCESYS
    FROM RSTRAN
    WHERE SOURCETYPE = 'RSDS'
    AND (UPPER(TRIM(SOURCE)) LIKE '%OPA%' OR UPPER(TRIM(SOURCESYS)) LIKE '%OPA%')
    LIMIT 20
""")
rows = cur.fetchall()
print(f"Found {len(rows)} RSDS records:")
for r in rows:
    print(f"  {r}")

conn.close()
