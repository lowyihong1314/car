#!/usr/bin/env python3
from __future__ import annotations

import argparse

from crawler.sqlite_ops import import_xlsx_into_db, resolve_user_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import Carlist_Type_Mapping_RAW.xlsx into the SQLite crawler database."
    )
    parser.add_argument(
        "--xlsx",
        default="../Carlist_Type_Mapping_RAW.xlsx",
        help="Path to the source workbook.",
    )
    parser.add_argument(
        "--db",
        default="carlist_type_mapping_python.db",
        help="Path to the target SQLite database.",
    )
    args = parser.parse_args()

    xlsx_path = resolve_user_path(args.xlsx)
    db_path = resolve_user_path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    imported_rows = import_xlsx_into_db(db_path, xlsx_path)
    print(f"Imported {imported_rows} row(s) from {xlsx_path} into {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
