#!/usr/bin/env python3
"""Extract CDE baseline data for a specific district (for injection into prompt)."""

import argparse
import json
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = REPO_DIR / "data" / "cde-districts.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("district_name", help="District name to look up")
    args = parser.parse_args()

    with open(DATA_FILE) as f:
        districts = json.load(f)

    # Find district (exact or fuzzy)
    target = None
    for d in districts:
        if d["name"].lower() == args.district_name.lower():
            target = d
            break

    if not target:
        for d in districts:
            if args.district_name.lower() in d["name"].lower():
                target = d
                break

    if not target:
        print(f"District '{args.district_name}' not found in CDE data.")
        return

    # Format as readable baseline for Claude
    print(f"## CDE Baseline Data: {target['name']}")
    print(f"- **CDS Code**: {target['cds_code']}")
    print(f"- **County**: {target['county']}")
    print(f"- **Type**: {target['type']}")
    print(f"- **Enrollment**: {target['enrollment']:,}")
    print(f"- **Superintendent**: {target['superintendent']}")
    print(f"- **Phone**: {target['phone']}")
    print()

    if target['demographics']:
        print("### Demographics")
        demo = target['demographics']
        for key in ['hispanic', 'white', 'asian', 'black', 'filipino',
                     'native_american', 'pacific_islander', 'two_or_more']:
            if key in demo:
                print(f"- {key.replace('_', ' ').title()}: {demo[key]}%")
        if 'sed_pct' in demo:
            print(f"- Socioeconomically Disadvantaged: {demo['sed_pct']}%")
        if 'el_pct' in demo:
            print(f"- English Learners: {demo['el_pct']}%")
        print()

    if target['caaspp_math']:
        print("### CAASPP Math (% Met/Exceeded Standard, 2023-24)")
        for key, val in sorted(target['caaspp_math'].items()):
            label = key.replace('_', ' ').title()
            print(f"- {label}: {val}%")
        print()

    if target['caaspp_ela']:
        print("### CAASPP ELA (% Met/Exceeded Standard, 2023-24)")
        for key, val in sorted(target['caaspp_ela'].items()):
            label = key.replace('_', ' ').title()
            print(f"- {label}: {val}%")
        print()

    if target['caaspp_science']:
        print("### CAST Science (% Met/Exceeded Standard, 2023-24)")
        for key, val in sorted(target['caaspp_science'].items()):
            label = key.replace('_', ' ').title()
            print(f"- {label}: {val}%")
        print()

    print(f"### Priority Score: {target['priority_score']}/100")


if __name__ == "__main__":
    main()
