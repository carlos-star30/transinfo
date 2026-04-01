import sqlite3

DB_PATH = 'backend/data/trans_fields_mapping.db'

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("Creating missing indexes...\n")

# Index for rsoadsot (ADSO object text lookup)
print("[1/3] Creating index on rsoadsot(ADSONM, OBJVERS, COLNAME)...")
try:
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rsoadsot_lookup 
        ON rsoadsot(ADSONM, OBJVERS, COLNAME)
    """)
    print("  ✓ rsoadsot index created")
except Exception as e:
    print(f"  ✗ Error: {e}")

# Index for rsdiobjt (IOBJ object text lookup)  
print("\n[2/3] Creating index on rsdiobjt(IOBJNM, OBJVERS)...")
try:
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rsdiobjt_lookup 
        ON rsdiobjt(IOBJNM, OBJVERS)
    """)
    print("  ✓ rsdiobjt index created")
except Exception as e:
    print(f"  ✗ Error: {e}")

# Index for rsoadso (ADSO activate data lookup)
print("\n[3/3] Creating index on rsoadso(ADSONM, OBJVERS)...")
try:
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rsoadso_lookup 
        ON rsoadso(ADSONM, OBJVERS)
    """)
    print("  ✓ rsoadso index created")
except Exception as e:
    print(f"  ✗ Error: {e}")

conn.commit()
conn.close()

print("\n✅ Done! Please restart the backend server.")
