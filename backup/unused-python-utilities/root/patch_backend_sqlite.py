#!/usr/bin/env python3
"""
Patch backend to support SQLite for local development.
This script modifies only the critical connection code and error handling.
"""
import os
import sys

backend_file = "backend/import_status_api.py"

# Read the file
with open(backend_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find and replace key sections
modified = False

# 1. Replace mysql.connector import with conditional import
for i, line in enumerate(lines):
    if line.strip() == "import mysql.connector":
        lines[i] = """import sqlite3
try:
    import mysql.connector
except ImportError:
    mysql.connector = None
"""
        modified = True
        print(f"✓ Patched line {i+1}: MySQL connector import")
        break

# 2. Find DB_CONFIG definition and add SQLite config
for i, line in enumerate(lines):
    if "DB_CONFIG" in line and "=" in line and "host" in lines[i+1] if i+1 < len(lines) else False:
        # Insert SQLite path config before DB_CONFIG
        lines.insert(i, """# SQLite configuration
import os
SQLITE_DB_PATH = os.environ.get("SQLITE_DB_PATH", "backend/data/trans_fields_mapping.db")
IS_SQLITE = os.environ.get("DB_TYPE") == "sqlite"

""")
        modified = True
        print(f"✓ Added SQLite config before line {i+1}")
        break

# 3. Find get_conn() function and wrap it
for i, line in enumerate(lines):
    if "def get_conn():" in line:
        # Find the end of function (next def or class)
        end_i = i + 1
        for j in range(i+1, min(i+20, len(lines))):
            if lines[j].strip().startswith("def ") or lines[j].strip().startswith("class "):
                end_i = j
                break
        
        # Replace the function
        indent = "    "
        new_func = f"""{indent}if IS_SQLITE:
{indent}    conn = sqlite3.connect(SQLITE_DB_PATH, check_same_thread=False)
{indent}    conn.row_factory = sqlite3.Row
{indent}    return conn
{indent}else:
{indent}    return mysql.connector.connect(**DB_CONFIG)

"""
        # Find actual connection line
        for k in range(i, end_i):
            if "mysql.connector.connect" in lines[k]:
                # Replace this line
                lines[k:k+1] = [new_func]
                modified = True
                print(f"✓ Patched get_conn() at line {k+1}")
                break
        break

if modified:
    # Write back
    with open(backend_file, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print(f"\n✓ Backend patched successfully")
else:
    print(f"✗ Could not find/patch the necessary code sections")
    print(f"  Ensure the backend file has mysql.connector import and DB_CONFIG definition")
    sys.exit(1)
