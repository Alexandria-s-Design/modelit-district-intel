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
import random
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
MICROMAYHEM_VIDEO = "https://youtu.be/9-t5l9MR95w"

# Role-based subject line templates. {district} is replaced at send time.
SUBJECT_TEMPLATES = {
    "superintendent": "Free NGSS modeling platform — {district} science",
    "curriculum": "NGSS computational modeling for {district} science classrooms",
    "principal": "Free science modeling tool for your students — NGSS-aligned",
    "teacher": "Free computational modeling platform for your science class",
    "tech": "Browser-based NGSS platform — Chromebook-ready, no install",
    "board": "NSF-funded science platform available for {district} students",
    "budget": "Free NGSS science platform — no cost, no trial period",
    "default": "NGSS computational modeling for {district} science classrooms",
}

# Keywords in titles that map to role categories
# Order matters — more specific roles checked first to avoid false matches
ROLE_KEYWORDS = {
    "superintendent": ["superintendent", r"\bsupt\b"],
    "budget": [r"\bbusiness\b", r"\bcbo\b", "chief business", r"\bfinance\b",
               "federal program", "state.*federal", r"\bpurchasing\b"],
    "tech": [r"\btechnology\b", r"\bcto\b", r"\bit director\b", r"\bit coordinator\b",
             "chief technology", "information technology"],
    "curriculum": ["curriculum", "instruction", "tosa", "specialist", "coordinator",
                    "director.*education", "director.*instruction", "content area",
                    "academic", "expanded learning", "ell", "multilingual", "eld"],
    "principal": ["principal", "assistant principal"],
    "teacher": ["teacher", "department chair", "science lead"],
    "board": ["trustee", r"\bboard\b", r"\bclerk\b"],
}

# Fake/template emails that should never be sent
FAKE_EMAIL_PATTERNS = [
    "firstlast@", "firstname_lastname@", "firstnamelastinitial@",
    "flast@", "lastname+first@", "lastname_firstname@",
    "lastnamefirstinitial@", "firstname.lastname@example",
    "lastname+firsttwoletters@", "lastnamefirstinitial@",
]

# Generic mailboxes that rarely generate responses in cold outreach
GENERIC_EMAIL_PREFIXES = [
    "boardoftrustees@", "communications@", "contact@", "info@",
    "theofficeofthesuperintendent@", "front@",
]

# Sections in contacts.md that contain example patterns, not real contacts
SKIP_SECTIONS = [
    "email pattern analysis", "email pattern", "quick reference",
    "contact count summary", "verification needed",
]



def clean_district_name(name):
    """Strip internal suffixes like Intelligence Profile that should never appear in emails."""
    name = re.sub(r"\s*[-\u2014\u2013]+\s*(?:Full\s+)?(?:District\s+)?Intelligence\s+Profile\s*$", "", name, flags=re.IGNORECASE)
    return name.strip()

def load_sent():
    if SENT_TRACKER.exists():
        return set(json.loads(SENT_TRACKER.read_text()).get("sent", []))
    return set()


def save_sent(sent_set):
    SENT_TRACKER.write_text(json.dumps({"sent": sorted(sent_set), "updated": datetime.now(timezone.utc).isoformat()}, indent=2))


def is_fake_email(email):
    """Check if an email is a template/pattern example, not a real address."""
    email_lower = email.lower()
    for pattern in FAKE_EMAIL_PATTERNS:
        if pattern in email_lower:
            return True
    return False


def is_generic_email(email):
    """Check if an email is a generic office mailbox."""
    email_lower = email.lower()
    for prefix in GENERIC_EMAIL_PREFIXES:
        if email_lower.startswith(prefix):
            return True
    # Also catch office.xxx@ patterns
    if re.match(r"office\.\w+@", email_lower):
        return True
    return False


def is_in_skip_section(section_header):
    """Check if we are inside a section that contains examples, not real contacts."""
    header_lower = section_header.lower()
    return any(skip in header_lower for skip in SKIP_SECTIONS)


def clean_contact_name(raw_name):
    """Extract just the person's name from a header like '### Korina Tabarez — PRIMARY CONTACT'."""
    # Remove markdown bold
    name = re.sub(r"\*\*(.+?)\*\*", r"\1", raw_name)
    # Remove leading # symbols and whitespace
    name = re.sub(r"^#+\s*", "", name)
    # Remove numbering like "1." or "1 |" at the start
    name = re.sub(r"^\d+\.?\s*\|?\s*", "", name)
    # Split on em-dash/en-dash to remove role/description suffix
    name = re.split(r"\s*[\u2014\u2013]+\s+", name)[0]
    # Also handle " — " with regular dash
    name = re.split(r"\s+—\s+", name)[0]
    # Remove trailing degree suffixes
    name = re.sub(r",?\s*(?:Ed\.?D\.?|Ph\.?D\.?|M\.?D\.?)\.?\s*$", "", name, flags=re.IGNORECASE)
    return name.strip()


def extract_first_email(text):
    """Extract the first valid email from text, ignoring pattern descriptions."""
    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    for email in emails:
        if not is_fake_email(email):
            return email
    return ""


def parse_contacts(district_slug):
    """Parse contacts from markdown files that use table format (| Field | Value |)
    and also structured header format (### Name — Role)."""
    path = REPO_DIR / "districts" / district_slug / "contacts.md"
    if not path.exists() or path.stat().st_size == 0:
        return []
    content = path.read_text(encoding="utf-8")
    contacts = []
    current = {}
    current_section = ""
    in_skip_section = False
    in_inline_table = False  # For compact tables like | Name | School | Email | ...
    inline_table_headers = []

    for line in content.split("\n"):
        stripped = line.strip()

        # Track section headers (## or ###)
        if stripped.startswith("## ") or stripped.startswith("### "):
            # Save previous contact if it has an email
            if current.get("email") and not is_fake_email(current["email"]):
                contacts.append(current)
            in_inline_table = False
            inline_table_headers = []

            # Check if this is a skip section (Email Pattern Analysis, etc.)
            if stripped.startswith("## "):
                current_section = stripped
                in_skip_section = is_in_skip_section(stripped)
                current = {}
                continue

            if in_skip_section:
                current = {}
                continue

            # Parse name from ### header (e.g., "### Korina Tabarez — PRIMARY CONTACT")
            raw = re.sub(r"^#+\s*", "", stripped)
            name = clean_contact_name(raw)
            current = {"name": name, "title": "", "email": "", "hook": "", "notes": ""}
            continue

        if in_skip_section:
            continue

        # Handle table format: | Field | Value |
        if stripped.startswith("|") and "|" in stripped[1:]:
            cells = [c.strip() for c in stripped.split("|")[1:-1]]  # Remove empty first/last from split

            # Detect if this is a header row for an inline table (compact multi-contact tables)
            if len(cells) >= 3 and any(h.lower() in ["name", "email", "school", "title", "role"] for h in cells):
                # Check if it's a separator row
                if all(re.match(r"^[-:]+$", c) for c in cells):
                    continue
                inline_table_headers = [h.lower().strip() for h in cells]
                in_inline_table = True
                continue

            # Skip separator rows (|---|---|)
            if all(re.match(r"^[-:]+$", c) for c in cells if c):
                continue

            # Process inline table rows (compact tables with Name | Email | etc.)
            if in_inline_table and inline_table_headers and len(cells) >= len(inline_table_headers):
                row = {}
                for i, header in enumerate(inline_table_headers):
                    if i < len(cells):
                        row[header] = cells[i]

                row_name = row.get("name", "").strip()
                row_email = ""
                for key in ["email", "e-mail"]:
                    if key in row and row[key].strip():
                        row_email = extract_first_email(row[key])
                        break

                if row_email and row_name:
                    # Clean name (remove bold markers)
                    row_name = re.sub(r"\*\*(.+?)\*\*", r"\1", row_name)
                    row_title = row.get("title", row.get("role", row.get("position", ""))).strip()
                    row_title = re.sub(r"\*\*(.+?)\*\*", r"\1", row_title)
                    contacts.append({
                        "name": row_name,
                        "title": row_title,
                        "email": row_email,
                        "hook": row.get("pitch hook", row.get("message focus", "")),
                        "notes": row.get("notes", ""),
                    })
                continue

            # Process Field | Value table rows (2-column detail tables)
            if len(cells) == 2:
                field = cells[0].lower().strip()
                value = cells[1].strip()

                if field in ("title", "position"):
                    current["title"] = value
                elif field == "email":
                    email = extract_first_email(value)
                    if email:
                        current["email"] = email
                elif field in ("pitch hook", "hook"):
                    current["hook"] = value
                elif field in ("role", "role in decision"):
                    if not current["title"]:
                        current["title"] = value
                elif field == "message focus":
                    if not current["hook"]:
                        current["hook"] = value
            continue

        # Also handle the old list format as fallback:
        # Handles both "- **Email**: value" and "- **Email:** value"
        if stripped.lower().startswith("- **"):
            # Extract value after the bold field label — handles **X**: val and **X:** val
            def extract_list_value(line):
                m = re.search(r"\*\*:?\s*(.+)", line)
                if m:
                    val = m.group(1).strip()
                    # Remove leading colon if present (from **Email:** pattern)
                    val = re.sub(r"^:\s*", "", val)
                    return val
                return ""

            lowered = stripped.lower()
            if lowered.startswith("- **title") or lowered.startswith("- **why"):
                val = extract_list_value(stripped)
                if val and not current["title"]:
                    current["title"] = val
            elif lowered.startswith("- **email"):
                val = extract_list_value(stripped)
                if val:
                    email = extract_first_email(val)
                    if email: current["email"] = email
            elif lowered.startswith("- **pitch hook") or lowered.startswith("- **hook"):
                val = extract_list_value(stripped)
                if val: current["hook"] = val

    # Don't forget last contact
    if current.get("email") and not is_fake_email(current["email"]):
        contacts.append(current)

    # Filter out fake emails, generic mailboxes, and TBD/unknown contacts
    valid_contacts = []
    seen_emails = set()
    for c in contacts:
        email = c["email"].lower()
        if email in seen_emails:
            continue
        if is_fake_email(email):
            continue
        if is_generic_email(email):
            continue
        # Skip contacts with placeholder names
        name_lower = c.get("name", "").lower()
        if "tbd" in name_lower or "principal tbd" in name_lower or name_lower.startswith("["):
            continue
        seen_emails.add(email)
        valid_contacts.append(c)

    return valid_contacts


def load_district_profile(district_slug):
    profile_path = REPO_DIR / "districts" / district_slug / "profile.md"
    if profile_path.exists():
        content = profile_path.read_text(encoding="utf-8")
        hook_match = re.search(r"(?:Primary Hook|Key Hook|Pitch Hook|Opening Hook)[:\s]*(.+?)(?:\n|$)", content, re.IGNORECASE)
        district_name_match = re.search(r"#\s+(.+?)(?:\n|$)", content)
        return {
            "hook": hook_match.group(1).strip() if hook_match else "",
            "name": clean_district_name(district_name_match.group(1).strip()) if district_name_match else district_slug.replace("-", " ").title()
        }
    return {"hook": "", "name": district_slug.replace("-", " ").title()}


def classify_role(title):
    """Classify a contact's role based on their title for subject/CTA selection."""
    if not title:
        return "default"
    title_lower = title.lower()
    # Gatekeepers/assistants get generic treatment even if title mentions a VIP role
    if re.search(r"(executive assistant|secretary|gatekeeper|clerk(?!\s*pro))", title_lower):
        return "default"
    for role, keywords in ROLE_KEYWORDS.items():
        for kw in keywords:
            if re.search(kw, title_lower):
                return role
    return "default"


def get_subject(role, district_name):
    """Get the appropriate subject line for a contact's role."""
    template = SUBJECT_TEMPLATES.get(role, SUBJECT_TEMPLATES["default"])
    return template.format(district=district_name)


def get_greeting_name(full_name):
    """Extract the appropriate greeting name from a contact's full name.
    'Dr. Maria Lopez' -> 'Dr. Lopez', 'Korina Tabarez' -> 'Korina'."""
    if not full_name:
        return ""
    # Remove brackets, quotes, bold markers
    name = re.sub(r"[\[\]*\"']", "", full_name).strip()
    parts = name.split()
    if not parts:
        return ""
    # If name starts with Dr., use Dr. + last name
    if parts[0] in ("Dr.", "Dr"):
        if len(parts) >= 2:
            return f"Dr. {parts[-1]}"
        return ""
    return parts[0]


def get_cta(role, district_name):
    """Get a role-appropriate call-to-action."""
    ctas = {
        "superintendent": f"Would a 15-minute overview be useful? We can walk through how ModelIt fits {district_name}'s science goals.",
        "curriculum": f"Would you be open to a quick look? I can set up a free teacher preview account for {district_name} so you can see it firsthand.",
        "principal": "Would you like me to set up a free preview for one of your science teachers? No commitment — just a chance to see if it fits.",
        "teacher": "Want to try it? I can set up a free account for you in about 2 minutes — your students can start building models this week.",
        "tech": "Want to take a quick look at the platform? It runs entirely in-browser (Chrome, Safari, Edge) — no installation, no plugins, no IT tickets.",
        "board": f"If helpful, we would be glad to share a brief overview of how ModelIt could support {district_name}'s NGSS science goals.",
        "budget": f"ModelIt is completely free — no cost, no trial period, no strings. Happy to share more about how it fits {district_name}'s programs.",
        "default": f"Would you be open to a 15-minute look at how this could support {district_name}'s science goals?",
    }
    return ctas.get(role, ctas["default"])


def build_email_html(contact, district_name, district_hook):
    greeting_name = get_greeting_name(contact.get("name", ""))
    greeting = f"Hi {greeting_name}," if greeting_name else "Hello,"
    role = classify_role(contact.get("title", ""))

    hook = contact.get("hook") or district_hook or f"I noticed {district_name} is focused on strengthening STEM outcomes"
    # Clean hook: remove surrounding quotes if present
    hook = hook.strip('"').strip("'")

    cta = get_cta(role, district_name)

    html = f"""<html><body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto;">
<p>{greeting}</p>

<p>{hook} — and I wanted to share something that might be directly useful.</p>

<p><strong>ModelIt! K-12</strong> is a free computational modeling platform for science classrooms. Students build, run, and analyze dynamic models of biological and environmental systems — aligned to NGSS performance expectations.</p>

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

<p>What makes it different:</p>
<ul style="padding-left: 20px;">
<li><strong>Completely free</strong> — no cost, no trial period, no purchasing required</li>
<li><strong>Bilingual</strong> — full Spanish language support for EL students</li>
<li><strong>NSF SBIR-funded</strong> — developed with University of Nebraska-Lincoln</li>
<li><strong>Works on Chromebooks</strong> — browser-based, zero installation</li>
</ul>

<p>See it in action: <a href="{MICROMAYHEM_VIDEO}" style="color: #2997FF;">MicroMayhem game-based activity (2 min)</a></p>

<p>You can explore the full platform at <a href="https://modelitk12.com/#/" style="color: #2997FF;">modelitk12.com</a>.</p>

<p>{cta}</p>

<p>Best,</p>

<p style="margin: 0;"><strong>Dr. Marie Martin & Dr. Charles Martin</strong></p>
<p style="margin: 0; color: #666;">Discovery Collective | ModelIt! K-12</p>
<p style="margin: 0;"><a href="mailto:{FROM_EMAIL}" style="color: #2997FF;">{FROM_EMAIL}</a> &middot; <a href="https://modelitk12.com/#/" style="color: #2997FF;">modelitk12.com</a></p>
<p style="margin: 0; font-size: 12px; color: #888;">NSF SBIR Phase II | NGSS-Aligned | K-12 Computational Modeling</p>
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


def log_outreach(email, district, contact_name, status, message_id="", title="", role=""):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "email": email,
        "district": district,
        "contact_name": contact_name,
        "title": title,
        "role": role,
        "status": status,
        "message_id": message_id,
        "sender": FROM_EMAIL
    }
    with open(OUTREACH_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=20, help="Max emails to send per run")
    parser.add_argument("--delay", type=int, default=180, help="Base seconds between emails (randomized +/- 30s)")
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

        role = classify_role(contact.get("title", ""))
        subject = get_subject(role, district)
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
            log_outreach(email, slug, contact.get("name", ""), "sent", msg_id,
                         title=contact.get("title", ""), role=role)
            sent_count += 1
            print(f"SENT ({msg_id[:12]}...)")
        else:
            log_outreach(email, slug, contact.get("name", ""), "failed",
                         title=contact.get("title", ""), role=role)
            fail_count += 1
            print(f"FAILED: {result[:80]}")

        # Save progress after each email
        save_sent(sent_set)

        # Wait between emails with randomized delay (skip after last one)
        if i < len(batch) - 1:
            jitter = random.randint(-30, 30)
            actual_delay = max(60, args.delay + jitter)  # Never less than 60s
            print(f"    Waiting {actual_delay}s...", flush=True)
            time.sleep(actual_delay)

    print()
    print(f"=== Batch complete: {sent_count} sent, {fail_count} failed, {len(queue) - len(batch)} remaining ===")
    save_sent(sent_set)


if __name__ == "__main__":
    main()
