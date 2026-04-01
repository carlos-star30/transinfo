import re

file_path = 'frontend-prototype/app.js'

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Remove lines containing [PERF] console.log statements
original_count = len(lines)
lines = [line for line in lines if '[PERF]' not in line]
removed_count = original_count - len(lines)

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(lines)

print(f"Removed {removed_count} [PERF] log lines from {file_path}")
