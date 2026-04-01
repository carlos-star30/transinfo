import json

import requests

API = "http://localhost:8000/api/import/execute"

mapping = {
    "BW_OBJECT": "BW_OBJECT",
    "SOURCESYS": "SOURCESYS",
    "BW_OBJECT_TYPE": "BW_OBJECT_TYPE",
    "OBJECT_NAME": "OBJECT_NAME",
}

# 3-row sample to verify bw_object_name import without DATASOURCE column.
csv_payload = (
    "BW_OBJECT,SOURCESYS,BW_OBJECT_TYPE,OBJECT_NAME\n"
    "ZOBJ_SALES,P4C,ADSO,Sales Object\n"
    "ZOBJ_COST,E4C,ADSO,Cost Object\n"
    "ZOBJ_PLAN,P4C,IOBJ,Plan Object\n"
)

resp = requests.post(
    API,
    data={
        "table_name": "bw_object_name",
        "sheet_name": "",
        "mapping_json": json.dumps(mapping),
    },
    files={"file": ("bw_object_name_sample.csv", csv_payload, "text/csv")},
    timeout=30,
)

print("status:", resp.status_code)
print("body:", resp.text)
