#!/usr/bin/env python3
"""
ModelIt Batch Email Sender — 150/day with 2-minute spacing
Reads all contacts from researched districts, sends personalized emails
via gogcli from charles@discoverycollective.com with --client dc.

Usage: python3 modelit-batch-send.py [--batch 150] [--delay 120] [--dry-run]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_DIR = Path("/root/modelit-district-intel")
DATA_FILE = REPO_DIR / "data" / "cde-districts.json"
OUTREACH_LOG = REPO_DIR / "data" / "outreach-log.jsonl"
SENT_TRACKER = REPO_DIR / "data" / "batch-sent.json"

FROM_EMAIL = "charles@discoverycollective.com"
GOG_CLIENT = "dc"

IMG_BASE = "https://raw.githubusercontent.com/charlesmartinedd/modelit-district-intel/master/_reference/email-assets"
SCREENSHOT_BUILD = f"{IMG_BASE}/modelit-build-it.png"
SCREENSHOT_RUN = f"{IMG_BASE}/modelit-run-it.png"
MICROMAYHEM_VIDEO = "https://drive.google.com/file/d/1Xn6Ucu-tvC2wlttQhoCWHiyCqFxhiWnT/view?usp=drive_link"



def clean_district_name(name):
    """Strip internal suffixes like Intelligence Profile that should never appear in emails."""
    name = re.sub(r"\s*[-\u2014\u2013]+\s*(?:Full\s+)?(?:District\s+)?(?:Intelligence|District)\s+Profile\s*$", "", name, flags=re.IGNORECASE)
    return name.strip()

def load_sent():
    if SENT_TRACKER.exists():
        return set(json.loads(SENT_TRACKER.read_text()).get("sent", []))
    return set()


def save_sent(sent_set):
    SENT_TRACKER.write_text(json.dumps({"sent": sorted(sent_set), "updated": datetime.now(timezone.utc).isoformat()}, indent=2))


def is_template_email(email):
    """Filter out template/placeholder emails like firstname.lastname@domain."""
    local = email.split("@")[0].lower()
    templates = ["firstname", "lastname", "first.last", "first_last",
                 "finitial", "firsttwoinitials", "fullname", "firstinitiallast"]
    # Also filter personal emails and generic mailboxes
    personal_domains = ["gmail.com", "yahoo.com", "aol.com", "hotmail.com", "outlook.com"]
    generic_prefixes = ["askstudent", "info@", "noreply", "webmaster", "admin@"]
    domain = email.split("@")[1].lower() if "@" in email else ""
    if domain in personal_domains:
        return True
    if any(local.startswith(g.replace("@", "")) for g in generic_prefixes):
        return True
    return any(t in local for t in templates)


def extract_email_from_text(text):
    """Extract a real email address from text, stripping markdown formatting."""
    m = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    return m.group(0) if m else ""


def parse_contacts(district_slug):
    path = REPO_DIR / "districts" / district_slug / "contacts.md"
    if not path.exists() or path.stat().st_size == 0:
        return []
    content = path.read_text()
    contacts = []
    current = {}
    in_section = False
    for line in content.split("\n"):
        line = line.strip()
        if line.startswith("## ") or line.startswith("### "):
            if current.get("email"):
                contacts.append(current)
            name_match = re.search(r"#+\s+(.+?)(?:\s*[-—]|$)", line)
            if not name_match:
                name_match = re.search(r"#+\s+(.+)", line)
            current = {"name": name_match.group(1).strip() if name_match else "", "title": "", "email": "", "hook": "", "notes": ""}
            in_section = True
        # Dash-format: - **Email**: value
        elif line.lower().startswith("- **title"):
            m = re.search(r"\*\*:\s*(.+)", line)
            if m: current["title"] = m.group(1).strip()
        elif line.lower().startswith("- **email"):
            m = re.search(r"\*\*:\s*(.+)", line)
            if m: current["email"] = extract_email_from_text(m.group(1))
        elif line.lower().startswith("- **pitch hook") or line.lower().startswith("- **hook"):
            m = re.search(r"\*\*:\s*(.+)", line)
            if m: current["hook"] = m.group(1).strip()
        elif line.lower().startswith("- **role") or line.lower().startswith("- **position"):
            m = re.search(r"\*\*:\s*(.+)", line)
            if m and not current["title"]: current["title"] = m.group(1).strip()
        # Table-format: | Email | value | or | Title | value |
        elif in_section and "|" in line:
            cells = [c.strip() for c in line.split("|")]
            if len(cells) >= 3:
                key = cells[1].lower().strip()
                val = cells[2].strip()
                if key == "email" and not current.get("email"):
                    current["email"] = extract_email_from_text(val)
                elif key == "title" and not current.get("title"):
                    current["title"] = re.sub(r"\*", "", val).strip()
                elif key in ("pitch hook", "hook") and not current.get("hook"):
                    current["hook"] = re.sub(r'^"|"$', "", val).strip()
    if current.get("email"):
        contacts.append(current)
    # Fallback: extract any emails not captured by structured parsing
    all_emails = set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", content))
    parsed_emails = {c["email"].lower() for c in contacts}
    for email in all_emails:
        if email.lower() not in parsed_emails:
            contacts.append({"name": "", "title": "", "email": email, "hook": "", "notes": ""})
    # Filter out template/placeholder emails
    contacts = [c for c in contacts if c.get("email") and not is_template_email(c["email"])]
    return contacts


def load_district_profile(district_slug):
    profile_path = REPO_DIR / "districts" / district_slug / "profile.md"
    if profile_path.exists():
        content = profile_path.read_text()
        hook_match = re.search(r"(?:Primary Hook|Key Hook|Pitch Hook|Opening Hook)[:\s]*(.+?)(?:\n|$)", content, re.IGNORECASE)
        district_name_match = re.search(r"#\s+(.+?)(?:\n|$)", content)
        return {
            "hook": hook_match.group(1).strip() if hook_match else "",
            "name": clean_district_name(district_name_match.group(1).strip()) if district_name_match else district_slug.replace("-", " ").title()
        }
    return {"hook": "", "name": district_slug.replace("-", " ").title()}


def build_email_html(contact, district_name, district_hook):
    first_name = contact["name"].split()[0] if contact.get("name") else ""
    greeting = f"Hi {first_name}," if first_name else f"Hello,"

    hook = contact.get("hook") or district_hook or f"I noticed {district_name} is focused on strengthening STEM outcomes"

    html = f"""<html><body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto;">
<p>{greeting}</p>

<p>{hook} - and I wanted to share something that might be directly useful.</p>

<p><strong>ModelIt! K-12</strong> is a free computational modeling platform built specifically for science classrooms. Students build, run, and analyze dynamic models of biological and environmental systems - aligned to NGSS performance expectations.</p>

<p>Here is what it looks like in practice:</p>

<table style="width: 100%; margin: 16px 0;">
<tr>
<td style="width: 50%; padding: 4px; text-align: center;">
<img src="{SCREENSHOT_BUILD}" alt="Build a Model" style="max-width: 100%; border-radius: 8px; border: 1px solid #ddd;" />
<p style="font-size: 12px; color: #666;">Students build interactive models</p>
</td>
<td style="width: 50%; padding: 4px; text-align: center;">
<img src="{SCREENSHOT_RUN}" alt="Run Simulations" style="max-width: 100%; border-radius: 8px; border: 1px solid #ddd;" />
<p style="font-size: 12px; color: #666;">Then run real-time simulations</p>
</td>
</tr>
</table>

<p>A few things that make this different:</p>
<ul style="padding-left: 20px;">
<li><strong>Free for all teachers and students</strong> - no cost, no trial period</li>
<li><strong>Bilingual interface</strong> - full Spanish language support for EL students</li>
<li><strong>NSF-funded research</strong> - developed with University of Nebraska-Lincoln</li>
<li><strong>Works on Chromebooks</strong> - browser-based, no installation needed</li>
</ul>

<p>We are also developing <strong>MicroMayhem</strong> - a companion game that will be available on the Apple App Store and Google Play Store where students battle real biological threats using the same modeling skills they learn in class. Here is a 2-minute video of students playing it: <a href="{MICROMAYHEM_VIDEO}" style="color: #2997FF;">MicroMayhem Preview</a></p>

<p>Would you be open to a 15-minute look at how this could support {district_name}'s science goals? I can also set up a free teacher preview account.</p>

<p>Best,</p>

<p style="margin: 0;"><strong>Dr. Marie Martin & Dr. Charles Martin</strong></p>
<p style="margin: 0; color: #666;">Alexandria's Design | ModelIt! K-12</p>
<p style="margin: 0;"><a href="mailto:{FROM_EMAIL}" style="color: #2997FF;">Email</a> &middot; <a href="https://modelitk12.com/#/" style="color: #2997FF;">modelitk12.com</a></p>
<p style="margin: 0; font-size: 12px; color: #888;">NSF SBIR Phase II Funded | NGSS-Aligned | K-12 Computational Modeling</p>
</body></html>"""
    return html


def send_email(to_email, subject, html_body):
    tmp_file = Path("/tmp/modelit-email.html")
    tmp_file.write_text(html_body, encoding="utf-8")
    cmd = (
        f'gog gmail send --client {GOG_CLIENT} '
        f'--account {FROM_EMAIL} '
        f'--to "{to_email}" '
        f'--subject "{subject}" '
        f'--body-html "$(cat /tmp/modelit-email.html)"'
    )
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    tmp_file.unlink(missing_ok=True)
    if result.returncode == 0:
        return True, result.stdout.strip()
    return False, result.stderr.strip()


def log_outreach(email, district, contact_name, status, message_id=""):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "email": email,
        "district": district,
        "contact_name": contact_name,
        "status": status,
        "message_id": message_id,
        "sender": FROM_EMAIL
    }
    with open(OUTREACH_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=150, help="Max emails to send")
    parser.add_argument("--delay", type=int, default=120, help="Seconds between emails")
    parser.add_argument("--dry-run", action="store_true", help="Print without sending")
    args = parser.parse_args()

    sent_set = load_sent()
    print(f"Previously sent: {len(sent_set)} emails")

    # Build queue: all contacts from all researched districts, skip already sent
    queue = []
    for district_dir in sorted((REPO_DIR / "districts").iterdir()):
        if not district_dir.is_dir():
            continue
        slug = district_dir.name
        contacts = parse_contacts(slug)
        profile = load_district_profile(slug)
        for c in contacts:
            if c["email"].lower() in {s.lower() for s in sent_set}:
                continue
            queue.append({"contact": c, "slug": slug, "district_name": profile["name"], "district_hook": profile["hook"]})

    print(f"Emails in queue: {len(queue)}")
    print(f"Batch size: {args.batch}")
    print(f"Delay between emails: {args.delay}s ({args.delay/60:.1f} min)")
    print(f"Estimated time: {min(len(queue), args.batch) * args.delay / 60:.0f} minutes")
    print()

    batch = queue[:args.batch]
    sent_count = 0
    fail_count = 0

    for i, item in enumerate(batch):
        contact = item["contact"]
        email = contact["email"]
        district = item["district_name"]
        slug = item["slug"]

        subject = f"A quick look at computational modeling for {district} science"
        html = build_email_html(contact, district, item["district_hook"])

        name_display = contact.get("name", email)
        print(f"[{i+1}/{len(batch)}] {name_display} <{email}> ({district})", end=" ... ")

        if args.dry_run:
            print("DRY RUN - skipped")
            continue

        success, result = send_email(email, subject, html)
        if success:
            msg_id = ""
            for line in result.split("\n"):
                if "message_id" in line:
                    msg_id = line.split("\t")[-1].strip()
            sent_set.add(email.lower())
            log_outreach(email, slug, contact.get("name", ""), "sent", msg_id)
            sent_count += 1
            print(f"SENT ({msg_id[:12]}...)")
        else:
            log_outreach(email, slug, contact.get("name", ""), "failed")
            fail_count += 1
            print(f"FAILED: {result[:80]}")

        # Save progress after each email
        save_sent(sent_set)

        # Wait between emails (skip delay after last one)
        if i < len(batch) - 1:
            print(f"    Waiting {args.delay}s...", flush=True)
            time.sleep(args.delay)

    print()
    print(f"=== Batch complete: {sent_count} sent, {fail_count} failed, {len(queue) - len(batch)} remaining ===")
    save_sent(sent_set)


if __name__ == "__main__":
    main()
