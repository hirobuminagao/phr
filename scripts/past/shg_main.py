# C:\Users\1107858.KSMD\kenshin_list_pydir\shg_main.py

import os
import csv
import json
from lxml import etree
from datetime import datetime
import importlib.util
import importlib.abc
from typing import cast, Optional, Any, Dict

# 設定ファイルの読み込み
CONFIG_PATH = os.path.join("kenshin_lib", "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)

BASE_DIR = config["BASE_DIR"]
XML_ROOT_FOLDER = os.path.join(BASE_DIR, config["XML_FOLDERS"]["SHG"])
EXPORT_FOLDER = os.path.join(BASE_DIR, config["EXPORT_FOLDERS"]["SHG"])
OID_CSV_PATH = os.path.join(BASE_DIR, config["OID_CSV_PATH"])

# --- 安全版 import ユーティリティ -------------------------------------------
def load_module_from_path(module_name: str, module_path: str):
    """
    spec/loader の None 可能性に対処し、型安全にモジュールをロードする。
    """
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to create spec/loader for: {module_path}")
    module = importlib.util.module_from_spec(spec)
    loader = cast(importlib.abc.Loader, spec.loader)
    loader.exec_module(module)
    return module

# oid_utils.py の読み込み（安全版）
oid_utils_path = os.path.join(BASE_DIR, "kenshin_lib", "oid_utils.py")
oid_utils = load_module_from_path("oid_utils", oid_utils_path)

# OIDライブラリの読み込み
oid_library: Dict[str, Dict[str, str]] = oid_utils.load_oid_library(OID_CSV_PATH)

# 出力CSVの準備
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_CSV = os.path.join(EXPORT_FOLDER, f"shg_summary_{timestamp}.csv")

# ヘッダー定義（フォルダ名とファイル名の順序を入れ替え、生年月日XML形式を追加）
headers = [
    "フォルダ名", "ファイル名",
    "報告区分（コード）",
    "保険者番号", "保険証等記号", "保険証等番号",
    "対象者生年月日（XML形式）", "対象者生年月日",
    "初回面談日", "保健指導実施日",
    "保健指導レベル（コード）", "保健指導レベル（説明）",
    "エラー内容"
]

ns = {'hl7': 'urn:hl7-org:v3'}

def get_attr(tree: etree._ElementTree, xpath_expr: str, attr_name: str) -> str:
    result = tree.xpath(xpath_expr, namespaces=ns)
    if result:
        # result[0] は lxml の Element を想定
        return result[0].get(attr_name, "")  # type: ignore[no-any-return]
    return ""

def format_date(date_str: Optional[str]) -> str:
    if date_str and len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    return date_str or ""

results = []

# XMLファイルの処理（DATAフォルダ内のXMLのみ対象）
for subfolder in os.listdir(XML_ROOT_FOLDER):
    subfolder_path = os.path.join(XML_ROOT_FOLDER, subfolder)
    data_path = os.path.join(subfolder_path, "DATA")
    if not os.path.isdir(data_path):
        continue

    for file in os.listdir(data_path):
        if not file.lower().endswith(".xml"):
            continue

        filepath = os.path.join(data_path, file)
        try:
            tree = etree.parse(filepath)

            # 報告区分
            report_code = get_attr(tree, "/hl7:ClinicalDocument/hl7:code", "code")

            # 保健指導レベル（コードと説明）
            level_code = get_attr(
                tree,
                "//hl7:component//hl7:observation[hl7:code[@code='1020000001']]/hl7:value",
                "code",
            )
            level_desc = oid_library.get("1.2.392.200119.6.1112", {}).get(level_code, "")

            # その他の項目
            insurer = get_attr(
                tree,
                "//hl7:recordTarget/hl7:patientRole/hl7:id[@root='1.2.392.200119.6.101']",
                "extension",
            )
            symbol = get_attr(
                tree,
                "//hl7:recordTarget/hl7:patientRole/hl7:id[@root='1.2.392.200119.6.204']",
                "extension",
            )
            number = get_attr(
                tree,
                "//hl7:recordTarget/hl7:patientRole/hl7:id[@root='1.2.392.200119.6.205']",
                "extension",
            )

            birth_raw = get_attr(
                tree,
                "//hl7:recordTarget/hl7:patientRole/hl7:patient/hl7:birthTime",
                "value",
            )
            birth_formatted = format_date(birth_raw)

            interview = format_date(
                get_attr(
                    tree,
                    "//hl7:component//hl7:section[hl7:code[@code='90030']]/hl7:entry/hl7:act/hl7:effectiveTime",
                    "value",
                )
            )
            guidance_date = format_date(
                get_attr(
                    tree,
                    "//hl7:documentationOf/hl7:serviceEvent/hl7:effectiveTime",
                    "value",
                )
            )

            row = [
                os.path.basename(subfolder_path), file,
                report_code,
                insurer, symbol, number,
                birth_raw, birth_formatted,
                interview, guidance_date,
                level_code, level_desc, ""
            ]
            results.append(row)
        except Exception as e:
            results.append(
                [os.path.basename(subfolder_path), file]
                + [""] * (len(headers) - 3)
                + [str(e)]
            )

# CSV出力
os.makedirs(EXPORT_FOLDER, exist_ok=True)
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(results)

print(f"CSVファイルを出力しました: {OUTPUT_CSV}")
