from __future__ import annotations
import argparse
import json
from registry_delta_hub.service import run_fixture

def main() -> int:
    parser=argparse.ArgumentParser(description='四级肿瘤登记增量汇聚枢纽')
    parser.add_argument("fixture")
    parser.add_argument("--db",default="/tmp/registry_delta_hub.sqlite3")
    args=parser.parse_args()
    result=run_fixture(args.fixture,args.db)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True))
    return 0

if __name__ == "__main__": raise SystemExit(main())
