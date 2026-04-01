#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import sys

# Read the file with explicit UTF-8 handling
file_path = "backend/import_status_api.py"

try:
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    print(f"✓ Read file successfully ({len(content)} chars)")
except Exception as e:
    print(f"✗ Failed to read: {e}")
    sys.exit(1)

# List of replacements: (broken_string_pattern, replacement)
# Using partial matching due to encoding issues
replacements = [
    # Password validation messages (lines 356-362)
    (r'return\s+"瀵[^"]*小写[^"]*"', 'return "Password must contain lowercase letters"'),
    (r'return\s+"瀵[^"]*大写[^"]*"', 'return "Password must contain uppercase letters"'),
    (r'return\s+"瀵[^"]*数字[^"]*"', 'return "Password must contain numbers"'),
    (r'return\s+"瀵[^"]*特殊[^"]*"', 'return "Password must contain special characters"'),
    
    # Database comment (line 3103)
    (r"COMMENT='鐢\w+[^']*'", "COMMENT='User hidden objects list'"),
    
    # RSTRAN errors (lines 4486+)
    (r'detail="璺[^"]*RSTRAN[^"]*"', 'detail="Path query depends on RSTRAN data. Please import rstran table first."'),
    (r'detail="锟[^"]*Source[^"]*"', 'detail="When Source is a data source (RSDS), you must fill in Source system"'),
    
    # Full mode comment (line 5921)
    (r"\"\"\"Canonical app mode: full \([^)]*\)", "\"\"\"Canonical app mode: full (complete data)"),
    
    # Import errors (lines 6384+)
    (r'detail="瀵[^"]*Sheet[^"]*"', 'detail="Import failed: Cannot find specified sheet"'),
    (r'raise ValueError\(f"鏍[^"]*"\)', 'raise ValueError(f"Header row number {header_row_num} exceeds file valid range")'),
    
    # Login errors (lines 6516+)
    (r'detail="璇[^"]*密[^"]*"', 'detail="Please enter username and password"'),
    (r'detail="鐢[^"]*密[^"]*"', 'detail="Invalid username or password"'),
    (r'detail=f"鐧[^"]*分钟[^"]*"', 'detail=f"Login attempts exceeded. Please try again in {remain_minutes} minutes"'),
    (r'detail="鐢[^"]*管理[^"]*"', 'detail="User account is locked. Please contact administrator"'),
    (r'content=\{"detail":\s*"嗦[^"]*expire[^"]*"\}', 'content={"detail": "Not logged in or session expired"}'),
]

# Apply replacements
modified = False
for pattern, replacement in replacements:
    if re.search(pattern, content):
        new_content = re.sub(pattern, replacement, content)
        if new_content != content:
            print(f"✓ Fixed pattern: {replacement[:50]}...")
            content = new_content
            modified = True

# Additional raw string replacements for problematic lines
raw_replacements = [
    ("return f\"Password must be at least {AUTH_PASSWORD_MIN_LEN} characters\"\n    if not any(c.islower() for c in raw):\n        return \"瀵", 
     "return f\"Password must be at least {AUTH_PASSWORD_MIN_LEN} characters\"\n    if not any(c.islower() for c in raw):\n        return \"Password must contain lowercase letters"),
]

for old, new_pattern in raw_replacements:
    # This is too risky, skip for now
    pass

if modified:
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"\n✓ File updated successfully")
    except Exception as e:
        print(f"\n✗ Failed to write: {e}")
        sys.exit(1)
else:
    print("\n⚠ No replacements made - all patterns already fixed or not found")
