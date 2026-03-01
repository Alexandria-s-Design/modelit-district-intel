#!/usr/bin/env python3
"""
ModelIt Daily Outreach — Email + HubSpot Sync
Reads newly-researched district profiles, sends personalized outreach emails
via gogcli, creates HubSpot contacts/deals, and logs email in HubSpot.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = REPO_DIR / "data" / "cde-districts.json"
OUTREACH_LOG = REPO_DIR / "data" / "outreach-log.jsonl"
SCRIPTS_DIR = REPO_DIR / "scripts"

# HubSpot API
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
if not HUBSPOT_TOKEN:
    print("ERROR: HUBSPOT_TOKEN environment variable not set")
    sys.exit(1)
HUBSPOT_API = "https://api.hubapi.com"

# Telegram group for notifications
TELEGRAM_GROUP = os.environ.get("TELEGRAM_GROUP", "-5163496634")

# Email sender
FROM_EMAIL = "charles@discoverycollective.com"
GOG_CLIENT = "dc"

# Email assets (hosted URLs for inline images)
SCREENSHOT_1_URL = "https://raw.githubusercontent.com/charlesmartinedd/modelit-district-intel/main/_reference/email-assets/modelit-platform.png"
SCREENSHOT_2_URL = "https://raw.githubusercontent.com/charlesmartinedd/modelit-district-intel/main/_reference/email-assets/modelit-student.png"
VIDEO_URL = "https://drive.google.com/file/d/1Jx-MicroMayhemPromo/view"  # Update with actual link


def run_cmd(cmd, check=True):
    """Run a shell command and return output."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"CMD FAILED: {cmd}\n{result.stderr}")
    return result


def hubspot_request(method, endpoint, data=None):
    """Make a HubSpot API request via curl."""
    url = f"{HUBSPOT_API}{endpoint}"
    cmd = f'curl -s -X {method} "{url}" -H "Authorization: Bearer {HUBSPOT_TOKEN}" -H "Content-Type: application/json"'
    if data:
        json_str = json.dumps(data).replace('"', '\\"')
        cmd += f' -d "{json_str}"'

    # Use Python requests-style approach via subprocess
    import urllib.request
    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization", f"Bearer {HUBSPOT_TOKEN}")
    req.add_header("Content-Type", "application/json")
    if data:
        req.data = json.dumps(data).encode()
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"HubSpot API error: {e}")
        return None


def parse_entry_strategy(district_slug):
    """Parse entry-strategy.md for contact info and hook."""
    path = REPO_DIR / "districts" / district_slug / "entry-strategy.md"
    if not path.exists():
        return None

    content = path.read_text(encoding='utf-8', errors='replace')

    # Extract primary contact table
    contact = {}
    table_match = re.search(r'## Primary Contact\s*\n\n\|.*\n\|.*\n((?:\|.*\n)*)', content)
    if table_match:
        for line in table_match.group(1).strip().split('\n'):
            parts = [p.strip() for p in line.split('|') if p.strip()]
            if len(parts) >= 2:
                key = parts[0].lower()
                val = parts[1]
                if key == 'name':
                    contact['name'] = val
                elif key == 'title':
                    contact['title'] = val
                elif key == 'email':
                    contact['email'] = val

    # Extract The Hook
    hook_match = re.search(r'## The Hook\s*\n\n> "(.+?)"', content, re.DOTALL)
    if hook_match:
        contact['hook'] = hook_match.group(1).strip().replace('\n> ', ' ')

    return contact if contact.get('email') else None


def build_email_html(district_name, contact):
    """Build personalized HTML email body."""
    first_name = contact['name'].split()[0] if contact.get('name') else 'there'
    hook = contact.get('hook', f'I noticed {district_name} has some exciting STEM initiatives, and I wanted to share something that might complement your work.')

    # Trim hook to 2-3 sentences
    sentences = hook.split('. ')
    if len(sentences) > 3:
        hook = '. '.join(sentences[:3]) + '.'

    html = f"""<html><body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
<p>Hi {first_name},</p>

<p>{hook}</p>

<p>ModelIt is a computational modeling platform for K-12 that lets students build, test, and explore scientific models — no coding required. We're also developing a game that brings these modeling concepts into an interactive experience students love.</p>

<p><img src="{SCREENSHOT_1_URL}" alt="ModelIt Platform" style="max-width: 500px; border: 1px solid #ddd; border-radius: 4px;" /></p>

<p>Here's a quick look at what students can do:<br/>
<a href="{VIDEO_URL}" style="color: #2997FF;">Watch the MicroMayhem Promo (1 min)</a></p>

<p>If this looks like something worth exploring for {district_name}, just reply "preview" and I'll send over a hands-on demo link.</p>

<p>Best,<br/>
Dr. Charles Martin &amp; Dr. Marie Martin<br/>
Discovery Collective / ModelIt<br/>
<a href="mailto:{FROM_EMAIL}">{FROM_EMAIL}</a></p>
</body></html>"""
    return html


def send_email(to_email, subject, html_body):
    """Send email via gogcli."""
    # Write HTML to temp file to avoid shell escaping issues
    tmp_file = Path("/tmp/modelit-email.html")
    tmp_file.write_text(html_body, encoding='utf-8')

    cmd = (f'gog gmail send --client {GOG_CLIENT} '
           f'--account {FROM_EMAIL} '
           f'--to "{to_email}" '
           f'--subject "{subject}" '
           f'--body-html-file "{tmp_file}"')

    result = run_cmd(cmd, check=False)
    tmp_file.unlink(missing_ok=True)

    if result.returncode == 0:
        # Try to extract message ID from output
        msg_id = ""
        for line in result.stdout.split('\n'):
            if 'id' in line.lower() or 'message' in line.lower():
                msg_id = line.strip()
                break
        return True, msg_id
    else:
        return False, result.stderr


def create_hubspot_contact(contact, district_name):
    """Create a HubSpot contact."""
    first_name = contact['name'].split()[0] if contact.get('name') else ''
    last_name = ' '.join(contact['name'].split()[1:]) if contact.get('name') and len(contact['name'].split()) > 1 else ''

    data = {
        "properties": {
            "email": contact['email'],
            "firstname": first_name,
            "lastname": last_name,
            "jobtitle": contact.get('title', ''),
            "company": district_name,
            "contact_segment": "leadership",
            "lead_temperature": "warm",
            "contact_attempt": "1"
        }
    }

    result = hubspot_request("POST", "/crm/v3/objects/contacts", data)
    if result and 'id' in result:
        print(f"  HubSpot contact created: {result['id']}")
        return result['id']
    elif result and 'message' in result and 'already exists' in result.get('message', '').lower():
        print(f"  HubSpot contact already exists")
        # Try to find existing contact
        search_data = {"filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": contact['email']}]}]}
        search_result = hubspot_request("POST", "/crm/v3/objects/contacts/search", search_data)
        if search_result and search_result.get('results'):
            return search_result['results'][0]['id']
    return None


def create_hubspot_deal(district_name, contact_id, amount=15000):
    """Create a HubSpot deal and associate to contact."""
    data = {
        "properties": {
            "dealname": f"{district_name} - ModelIt K12 Pilot",
            "dealstage": "appointmentscheduled",
            "amount": str(amount),
            "pipeline": "default"
        }
    }

    result = hubspot_request("POST", "/crm/v3/objects/deals", data)
    if result and 'id' in result:
        deal_id = result['id']
        print(f"  HubSpot deal created: {deal_id}")

        # Associate deal to contact
        if contact_id:
            assoc_data = [{"to": {"id": contact_id}, "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 3}]}]
            hubspot_request("PUT", f"/crm/v4/objects/deals/{deal_id}/associations/contacts", assoc_data)

        return deal_id
    return None


def log_email_hubspot(contact_id, subject, html_body):
    """Log sent email in HubSpot."""
    data = {
        "properties": {
            "hs_email_direction": "EMAIL",
            "hs_email_status": "SENT",
            "hs_email_subject": subject,
            "hs_email_text": html_body[:5000],
            "hs_timestamp": datetime.now(timezone.utc).isoformat()
        }
    }

    result = hubspot_request("POST", "/crm/v3/objects/emails", data)
    if result and 'id' in result and contact_id:
        email_id = result['id']
        assoc_data = [{"to": {"id": contact_id}, "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 198}]}]
        hubspot_request("PUT", f"/crm/v4/objects/emails/{email_id}/associations/contacts", assoc_data)
        print(f"  HubSpot email logged: {email_id}")


def send_telegram(message):
    """Send a message to the Telegram group via OpenClaw."""
    cmd = f'openclaw send --to "{TELEGRAM_GROUP}" --message "{message}"'
    run_cmd(cmd, check=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=10, help="Number of districts to contact")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually send emails")
    args = parser.parse_args()

    # Load districts data
    with open(DATA_FILE) as f:
        districts = json.load(f)

    # Find districts that are researched but not yet contacted
    ready = [d for d in districts if d['status'] == 'researched'][:args.batch]

    if not ready:
        print("No researched districts ready for outreach.")
        return

    print(f"Outreach batch: {len(ready)} districts")
    sent_count = 0
    failed_count = 0

    for d in ready:
        district_name = d['name']
        slug = district_name.lower().replace(' ', '-').replace('.', '').replace("'", '')

        print(f"\n--- {district_name} ---")

        # Parse entry strategy for contact info
        contact = parse_entry_strategy(slug)
        if not contact:
            print(f"  SKIP: No entry-strategy.md or no email found for {slug}")
            failed_count += 1
            continue

        print(f"  Contact: {contact.get('name', 'Unknown')} <{contact['email']}>")

        # Build email
        subject = f"A quick look at computational modeling for {district_name.replace(' Unified', '').replace(' Elementary', '').replace(' School District', '')} science"
        html_body = build_email_html(district_name, contact)

        if args.dry_run:
            print(f"  DRY RUN: Would send to {contact['email']}")
            print(f"  Subject: {subject}")
            sent_count += 1
            continue

        # 1. Create HubSpot contact
        contact_id = create_hubspot_contact(contact, district_name)

        # 2. Create HubSpot deal
        deal_id = create_hubspot_deal(district_name, contact_id)

        # 3. Send email
        success, msg_info = send_email(contact['email'], subject, html_body)
        if success:
            print(f"  Email sent to {contact['email']}")

            # 4. Log email in HubSpot
            if contact_id:
                log_email_hubspot(contact_id, subject, html_body)

            # 5. Update master list
            for dd in districts:
                if dd['name'] == district_name:
                    dd['status'] = 'contacted'
                    dd['hubspot_contact_id'] = contact_id
                    dd['hubspot_deal_id'] = deal_id
                    break

            # 6. Log to outreach log
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "district": district_name,
                "contact_name": contact.get('name', ''),
                "contact_email": contact['email'],
                "subject": subject,
                "hubspot_contact_id": contact_id,
                "hubspot_deal_id": deal_id,
                "status": "sent"
            }
            with open(OUTREACH_LOG, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')

            sent_count += 1
        else:
            print(f"  FAILED: {msg_info}")
            failed_count += 1

    # Save updated districts
    with open(DATA_FILE, 'w') as f:
        json.dump(districts, f, indent=2, ensure_ascii=False)

    # Send Telegram summary
    summary = f"ModelIt Outreach: {sent_count} emails sent, {failed_count} failed"
    print(f"\n{summary}")
    send_telegram(summary)


if __name__ == "__main__":
    main()
