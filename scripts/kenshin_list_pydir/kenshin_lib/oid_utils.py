import csv

def load_oid_library(csv_path):
    oid_library = {}
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            oid = row.get("OID_code")
            code = row.get("OID_code_value")
            name = row.get("OID_code_value_name")
            if oid and code and name and oid != "OID_code" and code != "value":
                if oid not in oid_library:
                    oid_library[oid] = {}
                oid_library[oid][code.strip()] = name.strip()
    return oid_library
