#!/usr/bin/env python3
"""
ModelIt CDE Data Bootstrap
Downloads and indexes California Department of Education data files:
  1. Public Districts directory (names, contacts, websites, grade spans)
  2. CAASPP Smarter Balanced scores (ELA + Math by district, grade, subgroup)
  3. CAST science scores (by district)
  4. Cumulative enrollment demographics (ethnicity, EL, SED by district)

Outputs: data/cde-districts.json (~1,000 active K-12 districts with priority scores)
Run once, refresh monthly.
"""

import csv
import io
import json
import os
import sys
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

# ── Config ──────────────────────────────────────────────────────────────────
REPO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_DIR / "data"
CACHE_DIR = DATA_DIR / "_cache"

# CDE data URLs (2024-25 directory, 2023-24 assessments)
URLS = {
    "districts": "https://www.cde.ca.gov/SchoolDirectory/report?rid=dl2&tp=txt",
    "caaspp_ela": "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2024_all_csv_ela_v1.zip",
    "caaspp_math": "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2024_all_csv_math_v1.zip",
    "caaspp_entities": "https://caaspp-elpac.ets.org/caaspp/researchfiles/sb_ca2024entities_csv.zip",
    "cast": "https://caaspp-elpac.ets.org/caaspp/researchfiles/cast_ca2024_all_csv_v1.zip",
    "cast_entities": "https://caaspp-elpac.ets.org/caaspp/researchfiles/cast_ca2024entities_csv.zip",
    "enrollment": "https://www3.cde.ca.gov/demo-downloads/ce/cenroll2425.txt",
}

# CA state averages (2023-24 CAASPP) for priority scoring
STATE_AVG_MATH = 34.0  # % met or exceeded standard
STATE_AVG_ELA = 47.0
STATE_AVG_SCIENCE = 29.0

# Priority counties (SD/IE/LA metro)
PRIORITY_COUNTIES = {
    "San Diego", "San Bernardino", "Riverside", "Los Angeles",
    "Orange", "Imperial", "Ventura", "Kern"
}

# District type codes that indicate K-12 districts (from CDE DOC codes)
# 52=Elementary, 54=Unified, 56=High School
K12_DOC_CODES = {"52", "54", "56", "50"}


def download_file(url, dest):
    """Download a file if not cached."""
    if dest.exists():
        print(f"  Cached: {dest.name}")
        return
    print(f"  Downloading: {url}")
    urlretrieve(url, dest)
    print(f"  Saved: {dest.name} ({dest.stat().st_size / 1024 / 1024:.1f} MB)")


def extract_zip(zip_path):
    """Extract ZIP and return path to the CSV file inside."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(('.csv', '.txt'))]
        if not csv_files:
            raise ValueError(f"No CSV/TXT in {zip_path}")
        zf.extractall(CACHE_DIR)
        return CACHE_DIR / csv_files[0]


def read_csv_from_cache(name, delimiter='\t'):
    """Read a cached file as CSV."""
    path = CACHE_DIR / name
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader)


# ── Step 1: Parse Districts Directory ───────────────────────────────────────
def parse_districts():
    """Parse CDE public districts file into district records.
    File columns: CD Code, County, District, Street, City, Zip, State,
    MailStreet, MailCity, MailZip, MailState, Phone, Ext, FaxNumber,
    AdmFName, AdmLName, Latitude, Longitude, DOC, DOCType, StatusType, LastUpDate
    CD Code is 7 digits: 2-digit county + 5-digit district.
    """
    print("\n[1/4] Parsing districts directory...")
    dest = CACHE_DIR / "pubdist.txt"
    download_file(URLS["districts"], dest)

    districts = {}
    with open(dest, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            # Strip all keys (some have trailing whitespace like "Ext  ")
            row = {k.strip(): (v.strip() if v else "") for k, v in row.items()}

            cd_code = row.get("CD Code", "").strip()
            status = row.get("StatusType", "").strip()
            doc = row.get("DOC", "").strip()

            if not cd_code or len(cd_code) < 7:
                continue
            if status != "Active":
                continue
            if doc not in K12_DOC_CODES:
                continue

            county_code = cd_code[:2]
            district_code = cd_code[2:7]
            # Build 14-digit CDS code (append 7-digit school code 0000000)
            cds14 = f"{county_code}{district_code}0000000"

            name = row.get("District", "")
            county = row.get("County", "")

            districts[cds14] = {
                "name": name,
                "cds_code": f"{county_code}-{district_code}-0000000",
                "county": county,
                "type": _doc_to_type(doc),
                "enrollment": 0,
                "superintendent": _format_name(
                    row.get("AdmFName", ""),
                    row.get("AdmLName", "")
                ),
                "phone": row.get("Phone", ""),
                "website": "",  # Not in districts-only file
                "grade_span": "",  # Not in districts-only file
                "demographics": {},
                "caaspp_math": {},
                "caaspp_ela": {},
                "caaspp_science": {},
                "priority_score": 0,
                "status": "unresearched",
                "hubspot_contact_id": None,
                "hubspot_deal_id": None,
            }

    print(f"  Found {len(districts)} active K-12 districts")
    return districts


def _doc_to_type(doc):
    return {"52": "Elementary", "54": "Unified", "56": "High School", "50": "County Office"}.get(doc, "Other")


def _format_name(first, last):
    if first and last:
        return f"{first} {last}"
    return last or first or ""


# ── Step 2: Parse CAASPP Scores ─────────────────────────────────────────────
def parse_caaspp(districts):
    """Parse CAASPP ELA + Math research files for district-level scores."""
    print("\n[2/4] Parsing CAASPP scores (ELA + Math)...")

    # Download entities + score files
    for key in ["caaspp_entities", "caaspp_ela", "caaspp_math"]:
        dest = CACHE_DIR / f"{key}.zip"
        download_file(URLS[key], dest)

    # Build entity lookup (County_Code + District_Code → CDS)
    entities_zip = CACHE_DIR / "caaspp_entities.zip"
    entities_csv = extract_zip(entities_zip)

    entity_to_cds = {}
    with open(entities_csv, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='^')
        for row in reader:
            cc = row.get("County Code", row.get("County_Code", "")).strip().zfill(2)
            dc = row.get("District Code", row.get("District_Code", "")).strip().zfill(5)
            sc = row.get("School Code", row.get("School_Code", "")).strip().zfill(7)
            if sc == "0000000":  # district level
                cds14 = f"{cc}{dc}{sc}"
                entity_to_cds[(cc, dc)] = cds14

    # Parse ELA scores
    _parse_caaspp_subject(districts, entity_to_cds, "caaspp_ela", "caaspp_ela")
    # Parse Math scores
    _parse_caaspp_subject(districts, entity_to_cds, "caaspp_math", "caaspp_math")


def _parse_caaspp_subject(districts, entity_to_cds, zip_key, field_key):
    """Parse a CAASPP subject file and populate district scores.
    CSV is caret-delimited (^). Columns include:
      County Code, District Code, School Code, Student Group ID, Grade,
      Percentage Standard Met and Above
    """
    zip_path = CACHE_DIR / f"{zip_key}.zip"
    csv_path = extract_zip(zip_path)

    # Student Group IDs: 1=All, 3=Hispanic/Latino, 128=EL, 31=SED
    SUBGROUP_MAP = {"1": "overall", "3": "hispanic", "128": "el", "31": "sed"}
    # Grade: 3-8,11,13(all grades)
    GRADE_MAP = {"3": "grade_3", "4": "grade_4", "5": "grade_5",
                 "6": "grade_6", "7": "grade_7", "8": "grade_8",
                 "11": "grade_11", "13": "overall"}

    count = 0
    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='^')
        for row in reader:
            cc = row.get("County Code", "").strip().zfill(2)
            dc = row.get("District Code", "").strip().zfill(5)
            sc = row.get("School Code", "").strip().zfill(7)

            if sc != "0000000":  # only district level
                continue

            cds14 = f"{cc}{dc}{sc}"
            if cds14 not in districts:
                continue

            subgroup = row.get("Student Group ID", "").strip()
            grade = row.get("Grade", "").strip()
            pct_met = row.get("Percentage Standard Met and Above", "").strip()

            if not pct_met or pct_met == "*":
                continue

            try:
                pct = float(pct_met)
            except ValueError:
                continue

            sg_key = SUBGROUP_MAP.get(subgroup)
            gr_key = GRADE_MAP.get(grade)

            if sg_key and gr_key == "overall":
                # Overall score for this subgroup
                districts[cds14][field_key][sg_key] = pct
                count += 1
            elif sg_key == "overall" and gr_key:
                # All students, specific grade
                districts[cds14][field_key][gr_key] = pct
                count += 1

    print(f"  Loaded {count} {field_key} data points")


# ── Step 3: Parse CAST Science Scores ───────────────────────────────────────
def parse_cast(districts):
    """Parse CAST science research file for district-level scores."""
    print("\n[3/4] Parsing CAST science scores...")

    for key in ["cast_entities", "cast"]:
        dest = CACHE_DIR / f"{key}.zip"
        download_file(URLS[key], dest)

    cast_zip = CACHE_DIR / "cast.zip"
    csv_path = extract_zip(cast_zip)

    SUBGROUP_MAP = {"1": "overall", "3": "hispanic", "128": "el", "31": "sed"}
    count = 0

    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='^')
        for row in reader:
            cc = row.get("County Code", row.get("County_Code", "")).strip().zfill(2)
            dc = row.get("District Code", row.get("District_Code", "")).strip().zfill(5)
            sc = row.get("School Code", row.get("School_Code", "")).strip().zfill(7)

            if sc != "0000000":
                continue

            cds14 = f"{cc}{dc}{sc}"
            if cds14 not in districts:
                continue

            subgroup = row.get("Student Group ID", row.get("Subgroup_ID", "")).strip()
            grade = row.get("Grade", "").strip()
            pct_met = row.get("Percentage Standard Met and Above",
                              row.get("Percentage_Standard_Met_and_Above", "")).strip()

            if not pct_met or pct_met == "*":
                continue

            try:
                pct = float(pct_met)
            except ValueError:
                continue

            sg_key = SUBGROUP_MAP.get(subgroup)
            # CAST is grade 5, 8, HS — we want overall (grade 13)
            if sg_key and grade in ("13", "00"):
                districts[cds14]["caaspp_science"][sg_key] = pct
                count += 1

    print(f"  Loaded {count} CAST science data points")


# ── Step 4: Parse Enrollment Demographics ───────────────────────────────────
def parse_enrollment(districts):
    """Parse cumulative enrollment file for demographics."""
    print("\n[4/4] Parsing enrollment demographics...")
    dest = CACHE_DIR / "cenroll2425.txt"
    download_file(URLS["enrollment"], dest)

    # Reporting category codes
    ETHNICITY_MAP = {
        "RH": "hispanic", "RW": "white", "RA": "asian",
        "RB": "black", "RF": "filipino", "RP": "pacific_islander",
        "RI": "native_american", "RD": "two_or_more", "RT": "not_reported"
    }

    count = 0
    with open(dest, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            # Strip keys and values (handle None keys from extra delimiters)
            row = {(k.strip() if k else ""): (v.strip() if v else "")
                   for k, v in row.items() if k is not None}

            agg = row.get("AggregateLevel", "")
            if agg != "D":  # District level only
                continue

            cc = row.get("CountyCode", "").zfill(2)
            dc = row.get("DistrictCode", "").zfill(5)
            cds14 = f"{cc}{dc}0000000"

            if cds14 not in districts:
                continue

            cat = row.get("ReportingCategory", "")
            enroll_str = row.get("CumulativeEnrollment", "")

            if not enroll_str or enroll_str == "*":
                continue
            try:
                enroll = int(float(enroll_str))
            except ValueError:
                continue

            if cat == "TA":  # Total
                districts[cds14]["enrollment"] = enroll
                count += 1
            elif cat in ETHNICITY_MAP:
                districts[cds14]["demographics"][ETHNICITY_MAP[cat]] = enroll
            elif cat == "SE":  # Socioeconomically disadvantaged
                districts[cds14]["demographics"]["sed_count"] = enroll
            elif cat == "SM":  # English Learners (current)
                districts[cds14]["demographics"]["el_count"] = enroll

    # Convert counts to percentages
    for cds, d in districts.items():
        total = d["enrollment"]
        if total <= 0:
            continue
        demo = d["demographics"]
        for key in list(demo.keys()):
            if key.endswith("_count"):
                pct_key = key.replace("_count", "_pct")
                demo[pct_key] = round(demo[key] / total * 100, 1)
            elif key not in ("sed_pct", "el_pct"):
                # ethnicity counts → percentages
                demo[key] = round(demo[key] / total * 100, 1)

    print(f"  Loaded enrollment for {count} districts")


# ── Priority Scoring ────────────────────────────────────────────────────────
def compute_priority(districts):
    """Compute priority score (0-100) for each district."""
    print("\nComputing priority scores...")

    for cds, d in districts.items():
        score = 0

        # Math below state avg (+25)
        math_overall = d["caaspp_math"].get("overall")
        if math_overall is not None and math_overall < STATE_AVG_MATH:
            score += 25

        # High Hispanic enrollment (+20)
        hispanic_pct = d["demographics"].get("hispanic", 0)
        if hispanic_pct > 50:
            score += 20

        # Enrollment sweet spot 2K-50K (+15)
        enroll = d["enrollment"]
        if 2000 <= enroll <= 50000:
            score += 15
        elif 500 <= enroll < 2000:
            score += 5

        # Geographic proximity (+15)
        if d["county"] in PRIORITY_COUNTIES:
            score += 15

        # Science below state avg (+10)
        sci_overall = d["caaspp_science"].get("overall")
        if sci_overall is not None and sci_overall < STATE_AVG_SCIENCE:
            score += 10

        # ELA below state avg (+5 bonus)
        ela_overall = d["caaspp_ela"].get("overall")
        if ela_overall is not None and ela_overall < STATE_AVG_ELA:
            score += 5

        # Has superintendent listed (+5 — means we have a contact)
        if d["superintendent"]:
            score += 5

        # Has website (+5)
        if d["website"]:
            score += 5

        d["priority_score"] = min(score, 100)

    scored = [d for d in districts.values() if d["priority_score"] > 0]
    print(f"  {len(scored)} districts scored > 0 (max: {max(d['priority_score'] for d in districts.values())})")


# ── Merge Existing Profiles ─────────────────────────────────────────────────
def merge_existing(districts):
    """Mark already-profiled districts as 'researched'."""
    districts_dir = REPO_DIR / "districts"
    if not districts_dir.exists():
        return

    existing_slugs = [d.name for d in districts_dir.iterdir() if d.is_dir()]
    print(f"\nMerging {len(existing_slugs)} existing district profiles...")

    def _slugify(name):
        """Convert CDE district name to slug format matching existing dirs."""
        s = name.lower().replace(".", "").replace("'", "").strip()
        # Replace type suffixes with abbreviations used in slugs
        replacements = [
            (" unified school district", "-usd"), (" unified", "-usd"),
            (" elementary school district", "-esd"), (" elementary", "-esd"),
            (" union high school district", "-uhsd"), (" union high school", "-uhsd"),
            (" union school district", "-union-sd"), (" union", "-union-sd"),
            (" city school district", "-city-sd"), (" city elementary", "-city-sd"),
            (" high school district", "-hsd"),
            (" school district", "-sd"),
        ]
        for old, new in replacements:
            if s.endswith(old):
                s = s[:-len(old)] + new
                break
        return s.replace(" ", "-")

    # Build slug→cds lookup
    slug_to_cds = {}
    for cds, d in districts.items():
        slug = _slugify(d["name"])
        slug_to_cds[slug] = cds

    matched = 0
    for existing_slug in existing_slugs:
        # Try exact slug match
        if existing_slug in slug_to_cds:
            districts[slug_to_cds[existing_slug]]["status"] = "researched"
            matched += 1
            continue
        # Try substring matching
        for slug, cds in slug_to_cds.items():
            # Strip suffix and compare core names
            core_existing = existing_slug.rsplit("-", 1)[0] if "-" in existing_slug else existing_slug
            core_slug = slug.rsplit("-", 1)[0] if "-" in slug else slug
            if core_existing == core_slug or core_existing in slug or core_slug in existing_slug:
                districts[cds]["status"] = "researched"
                matched += 1
                break

    print(f"  Matched {matched}/{len(existing_slugs)} existing profiles")


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("ModelIt CDE Data Bootstrap")
    print("=" * 60)

    # Create directories
    DATA_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_ok=True)

    # Step 1-4: Download and parse
    districts = parse_districts()
    parse_caaspp(districts)
    parse_cast(districts)
    parse_enrollment(districts)

    # Priority scoring
    compute_priority(districts)

    # Merge existing profiles
    merge_existing(districts)

    # Convert to list sorted by priority
    output = sorted(districts.values(), key=lambda d: -d["priority_score"])

    # Write output
    out_path = DATA_DIR / "cde-districts.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n{'=' * 60}")
    print(f"Output: {out_path}")
    print(f"Total districts: {len(output)}")
    print(f"Researched: {sum(1 for d in output if d['status'] == 'researched')}")
    print(f"Unresearched: {sum(1 for d in output if d['status'] == 'unresearched')}")
    print(f"Top 10 by priority:")
    for d in output[:10]:
        print(f"  [{d['priority_score']:3d}] {d['name']} ({d['county']}) — "
              f"Enroll: {d['enrollment']:,}, Math: {d['caaspp_math'].get('overall', 'N/A')}%")
    print("=" * 60)


if __name__ == "__main__":
    main()
