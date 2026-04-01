import sqlite3
import sys
sys.path.insert(0, 'backend')

from import_status_api import resolve_rstran_start_name, normalize_bw_object_lookup

source_name = "0CO_OM_OPA_6"
source_system = "BP1CLNT010"

normalized_source = normalize_bw_object_lookup(source_name)
normalized_system = normalize_bw_object_lookup(source_system)

print(f"Source: {source_name} -> {normalized_source}")
print(f"System: {source_system} -> {normalized_system}")

resolved = resolve_rstran_start_name(normalized_source, normalized_system, "RSDS")
print(f"\nResolved start name: {resolved}")
