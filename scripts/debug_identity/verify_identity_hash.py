

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from pprint import pprint

# ------------------------------------------------------------
# script path bootstrap
# ------------------------------------------------------------
# `python scripts/debug_identity/verify_identity_hash.py ...`
# のような直実行でも `lib/...` を import できるようにする。
_THIS_FILE = Path(__file__).resolve()
_SCRIPTS_DIR = _THIS_FILE.parents[1]
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from lib.identity.builder.identity_hash import build_identity_hash
from lib.identity.builder.person_id_custom import build_person_id_custom
from lib.identity.field.birthdate import normalize_birthdate
from lib.identity.field.gender_code import normalize_gender_code
from lib.identity.field.insurance_number import normalize_insurance_number
from lib.identity.field.insurer_number import normalize_insurer_number
from lib.identity.field.insurance_symbol import normalize_insurance_symbol
from lib.identity.field.name_kana import normalize_name_kana_full


# ------------------------------------------------------------
# helpers
# ------------------------------------------------------------

def _status_label(ok: bool) -> str:
    return "OK" if ok else "NG"


# ------------------------------------------------------------
# main
# ------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="raw 値から person_id_custom / identity_hash を再生成して expected と比較する検証スクリプト"
    )
    parser.add_argument("--birthdate", required=True, help="生年月日 raw 値")
    parser.add_argument("--insurance-number", required=True, help="保険証番号 raw 値")
    parser.add_argument("--insurer-number", required=True, help="保険者番号 raw 値")
    parser.add_argument("--symbol", required=True, help="保険証記号 raw 値")
    parser.add_argument("--name-kana", required=True, help="氏名カナ raw 値")
    parser.add_argument("--gender-code", required=True, help="性別コード raw 値")
    parser.add_argument(
        "--expected-person-id-custom",
        default=None,
        help="DB に格納済みの期待 person_id_custom。指定した場合は一致判定を表示する",
    )
    parser.add_argument(
        "--expected-identity-hash",
        default=None,
        help="DB に格納済みの期待 identity_hash。指定した場合は一致判定を表示する",
    )
    parser.add_argument(
        "--resource-dir",
        default=None,
        help="custom_id_config.json / custom_id_mapping.json の配置先を明示したい場合に指定する",
    )
    args = parser.parse_args()

    # 1. field 正規化
    birthdate_result = normalize_birthdate(args.birthdate)
    insurance_number_result = normalize_insurance_number(args.insurance_number)
    insurer_number_result = normalize_insurer_number(args.insurer_number)
    insurance_symbol_result = normalize_insurance_symbol(args.symbol)
    name_kana_result = normalize_name_kana_full(args.name_kana)
    gender_code_result = normalize_gender_code(args.gender_code)

    # 2. person_id_custom builder 実行
    person_id_custom_result = build_person_id_custom(
        birthdate_match=birthdate_result.get("match"),
        insurance_number_match=insurance_number_result.get("match"),
        insurer_number_match=insurer_number_result.get("match"),
        insurance_symbol_person_id_custom=insurance_symbol_result.get("person_id_custom"),
        resource_dir=args.resource_dir,
    )

    # 3. identity_hash builder 実行
    identity_hash_result = build_identity_hash(
        person_id_custom=person_id_custom_result.get("value"),
        name_kana_full_match=name_kana_result.get("match"),
        gender_code_match=gender_code_result.get("match"),
    )

    # 4. 表示
    print("=" * 72)
    print("[INPUT RAW]")
    print(f"birthdate        : {args.birthdate}")
    print(f"insurance_number : {args.insurance_number}")
    print(f"insurer_number   : {args.insurer_number}")
    print(f"symbol           : {args.symbol}")
    print(f"name_kana        : {args.name_kana}")
    print(f"gender_code      : {args.gender_code}")
    print("=" * 72)

    print("[FIELD RESULT]")
    print(f"birthdate        : {_status_label(birthdate_result.get('ok', False))}")
    pprint(birthdate_result)
    print()

    print(f"insurance_number : {_status_label(insurance_number_result.get('ok', False))}")
    pprint(insurance_number_result)
    print()

    print(f"insurer_number   : {_status_label(insurer_number_result.get('ok', False))}")
    pprint(insurer_number_result)
    print()

    print(f"symbol           : {_status_label(insurance_symbol_result.get('ok', False))}")
    pprint(insurance_symbol_result)
    print()

    print(f"name_kana        : {_status_label(name_kana_result.get('ok', False))}")
    pprint(name_kana_result)
    print()

    print(f"gender_code      : {_status_label(gender_code_result.get('ok', False))}")
    pprint(gender_code_result)
    print("=" * 72)

    print("[BUILDER RESULT]")
    print(f"person_id_custom : {_status_label(person_id_custom_result.get('ok', False))}")
    pprint(person_id_custom_result)
    print()

    print(f"identity_hash    : {_status_label(identity_hash_result.get('ok', False))}")
    pprint(identity_hash_result)
    print("=" * 72)

    exit_code = 0

    if args.expected_person_id_custom is not None:
        generated = person_id_custom_result.get("value")
        expected = args.expected_person_id_custom
        is_same = generated == expected
        if not is_same:
            exit_code = 1

        print("[COMPARE person_id_custom]")
        print(f"generated : {generated}")
        print(f"expected  : {expected}")
        print(f"status    : {'OK' if is_same else 'NG'}")
        print("=" * 72)

    if args.expected_identity_hash is not None:
        generated = identity_hash_result.get("value")
        expected = args.expected_identity_hash
        is_same = generated == expected
        if not is_same:
            exit_code = 1

        print("[COMPARE identity_hash]")
        print(f"generated : {generated}")
        print(f"expected  : {expected}")
        print(f"status    : {'OK' if is_same else 'NG'}")
        print("=" * 72)

    if args.expected_person_id_custom is None and args.expected_identity_hash is None:
        if not person_id_custom_result.get("ok", False) or not identity_hash_result.get("ok", False):
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())