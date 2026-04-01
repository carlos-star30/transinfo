import json
import mysql.connector
import requests

API = "http://localhost:8000/api/import/execute"

mapping = {
    "TRANID": "TRANID",
    "OWNER": "OWNER",
    "SOURCENAME": "SOURCENAME",
    "SOURCESYS": "__LOGIC_SOURCENAME_SPLIT_LAST__",
    "DATASOURCE": "__LOGIC_SOURCENAME_SPLIT_FIRST__",
}

def query_rows():
    conn = mysql.connector.connect(host="localhost", port=3306, user="root", password="showlang", database="trans_fields_mapping")
    cur = conn.cursor()
    cur.execute("SELECT TRANID, OWNER FROM rstran ORDER BY TRANID")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

csv_ok = "TRANID,OWNER,SOURCENAME\nKEEP1,u1,DS1 SYS1\n"
csv_dup = "TRANID,OWNER,SOURCENAME\nDUPX,u1,DS1 SYS1\nDUPX,u2,DS2 SYS2\n"

r1 = requests.post(
    API,
    data={"table_name": "rstran", "sheet_name": "", "mapping_json": json.dumps(mapping)},
    files={"file": ("ok.csv", csv_ok, "text/csv")},
    timeout=30,
)
print("ok status", r1.status_code, r1.text)
print("after ok", query_rows())

r2 = requests.post(
    API,
    data={"table_name": "rstran", "sheet_name": "", "mapping_json": json.dumps(mapping)},
    files={"file": ("dup.csv", csv_dup, "text/csv")},
    timeout=30,
)
print("dup status", r2.status_code, r2.text)
print("after dup", query_rows())
