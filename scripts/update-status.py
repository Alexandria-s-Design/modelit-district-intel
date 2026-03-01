#!/usr/bin/env python3
"""Update a district's status in cde-districts.json."""

import argparse
import json
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = REPO_DIR / "data" / "cde-districts.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("district_name", help="District name to update")
    parser.add_argument("new_status", help="New status (researched, contacted, engaged, etc.)")
    parser.add_argument("--hubspot-contact-id", help="HubSpot contact ID")
    parser.add_argument("--hubspot-deal-id", help="HubSpot deal ID")
    args = parser.parse_args()

    with open(DATA_FILE) as f:
        districts = json.load(f)

    updated = False
    for d in districts:
        if d["name"].lower() == args.district_name.lower():
            d["status"] = args.new_status
            if args.hubspot_contact_id:
                d["hubspot_contact_id"] = args.hubspot_contact_id
            if args.hubspot_deal_id:
                d["hubspot_deal_id"] = args.hubspot_deal_id
            updated = True
            print(f"Updated {d['name']} → {args.new_status}")
            break

    if not updated:
        # Try fuzzy match
        for d in districts:
            if args.district_name.lower() in d["name"].lower():
                d["status"] = args.new_status
                updated = True
                print(f"Updated {d['name']} → {args.new_status} (fuzzy match)")
                break

    if not updated:
        print(f"ERROR: District '{args.district_name}' not found")
        return 1

    with open(DATA_FILE, 'w') as f:
        json.dump(districts, f, indent=2, ensure_ascii=False)

    return 0


if __name__ == "__main__":
    exit(main())
