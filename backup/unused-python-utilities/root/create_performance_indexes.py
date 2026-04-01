import sqlite3
import time

DB_PATH = 'backend/data/trans_fields_mapping.db'

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("Creating database indexes for performance optimization...\n")

# Index for rsoadsot (ADSO object text lookup)
print("[1/5] Creating index on rsoadsot(ADSONM, OBJVERS, COLNAME)...")
start = time.time()
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_rsoadsot_lookup 
    ON rsoadsot(ADSONM, OBJVERS, COLNAME)
""")
elapsed = time.time() - start
print(f"  ✓ Done ({elapsed:.2f}s)")

# Index for rsdiobjt (IOBJ object text lookup)
print("[2/5] Creating index on rsdiobjt(IOBJNM, OBJVERS)...")
start = time.time()
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_rsdiobjt_lookup 
    ON rsdiobjt(IOBJNM, OBJVERS)
""")
elapsed = time.time() - start
print(f"  ✓ Done ({elapsed:.2f}s)")

# Index for rstran (TRAN metadata lookup)
print("[3/5] Creating index on rstran(TRANID, OBJVERS)...")
start = time.time()
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_rstran_lookup 
    ON rstran(TRANID, OBJVERS)
""")
elapsed = time.time() - start
print(f"  ✓ Done ({elapsed:.2f}s)")

# Index for rstran (SOURCENAME, TARGETNAME lookup)
print("[4/5] Creating index on rstran(SOURCENAME, TARGETNAME)...")
start = time.time()
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_rstran_source_target 
    ON rstran(SOURCENAME, TARGETNAME)
""")
elapsed = time.time() - start
print(f"  ✓ Done ({elapsed:.2f}s)")

# Composite index for rstran_mapping_rule_full
print("[5/5] Creating composite index on rstran_mapping_rule_full(tran_id, rule_id, step_id)...")
start = time.time()
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_mapping_rule_full_composite 
    ON rstran_mapping_rule_full(tran_id, rule_id, step_id)
""")
elapsed = time.time() - start
print(f"  ✓ Done ({elapsed:.2f}s)")

conn.commit()
conn.close()

print("\n✅ All indexes created successfully!")
print("\nExpected performance improvements:")
print("  - fetch_adso_object_text_lookup: 1.3s → <0.1s")
print("  - fetch_iobj_object_text_lookup: 1.7s → <0.1s")
print("  - fetch_active_tran_metadata: slow → fast")
print("  - Overall mapping load: 7.4s → <2s")
