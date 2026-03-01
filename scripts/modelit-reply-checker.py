#!/usr/bin/env python3
"""
ModelIt Reply Checker — Detects replies to outreach emails.
Polls Gmail via gogcli, matches replies to outreach log, updates HubSpot, alerts Telegram.
"""

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
import urllib.request

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = REPO_DIR / "data" / "cde-districts.json"
OUTREACH_LOG = REPO_DIR / "data" / "outreach-log.jsonl"
REPLY_LOG = REPO_DIR / "data" / "reply-log.jsonl"

HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
HUBSPOT_API = "https://api.hubapi.com"
TELEGRAM_GROUP = os.environ.get("TELEGRAM_GROUP", "-5163496634")
FROM_EMAIL = "charles@discoverycollective.com"
GOG_CLIENT = "dc"


def hubspot_request(method, endpoint, data=None):
    """Make a HubSpot API request."""
    url = f"{HUBSPOT_API}{endpoint}"
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


def send_telegram(message):
    """Send Telegram notification."""
    cmd = f'openclaw send --to "{TELEGRAM_GROUP}" --message "{message}"'
    subprocess.run(cmd, shell=True, capture_output=True)


def get_recent_replies():
    """Get recent unread emails to charles@discoverycollective.com."""
    cmd = (f'gog gmail search --client {GOG_CLIENT} '
           f'--account {FROM_EMAIL} '
           f'--query "to:{FROM_EMAIL} is:unread newer_than:1d" '
           f'--json 2>/dev/null')

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Gmail search failed: {result.stderr}")
        return []

    try:
        data = json.loads(result.stdout)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'messages' in data:
            return data['messages']
        elif isinstance(data, dict) and 'results' in data:
            return data['results']
        return [data] if data else []
    except json.JSONDecodeError:
        # Try parsing as TSV/text
        lines = result.stdout.strip().split('\n')
        return [{"raw_line": line} for line in lines if line.strip()]


def load_outreach_log():
    """Load outreach log to match replies to contacts."""
    if not OUTREACH_LOG.exists():
        return {}

    contacts = {}
    with open(OUTREACH_LOG) as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                email = entry.get('contact_email', '').lower()
                if email:
                    contacts[email] = entry
            except json.JSONDecodeError:
                continue
    return contacts


def load_existing_replies():
    """Load already-processed reply IDs."""
    if not REPLY_LOG.exists():
        return set()

    ids = set()
    with open(REPLY_LOG) as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                if 'message_id' in entry:
                    ids.add(entry['message_id'])
            except json.JSONDecodeError:
                continue
    return ids


def main():
    print(f"ModelIt Reply Checker — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    outreach_contacts = load_outreach_log()
    existing_replies = load_existing_replies()

    if not outreach_contacts:
        print("No outreach log found. Nothing to check.")
        return

    print(f"Monitoring replies from {len(outreach_contacts)} contacts...")

    replies = get_recent_replies()
    new_replies = 0

    for msg in replies:
        # Extract sender email from message
        sender = ""
        msg_id = ""

        if isinstance(msg, dict):
            sender = msg.get('from', msg.get('sender', msg.get('From', ''))).lower()
            msg_id = msg.get('id', msg.get('message_id', ''))

            # Extract email from "Name <email>" format
            if '<' in sender and '>' in sender:
                sender = sender.split('<')[1].split('>')[0]
        elif isinstance(msg, str):
            continue

        if not sender or msg_id in existing_replies:
            continue

        # Check if sender is in our outreach contacts
        if sender in outreach_contacts:
            contact_info = outreach_contacts[sender]
            district = contact_info.get('district', 'Unknown')
            name = contact_info.get('contact_name', sender)
            contact_id = contact_info.get('hubspot_contact_id')
            deal_id = contact_info.get('hubspot_deal_id')

            print(f"\n  REPLY DETECTED: {name} at {district}")
            new_replies += 1

            # Update HubSpot deal stage
            if deal_id:
                update_data = {"properties": {"dealstage": "qualifiedtobuy"}}
                hubspot_request("PATCH", f"/crm/v3/objects/deals/{deal_id}", update_data)
                print(f"  HubSpot deal {deal_id} → qualifiedtobuy")

            # Update HubSpot contact temperature
            if contact_id:
                update_data = {"properties": {"lead_temperature": "hot"}}
                hubspot_request("PATCH", f"/crm/v3/objects/contacts/{contact_id}", update_data)
                print(f"  HubSpot contact {contact_id} → hot")

            # Update master list
            with open(DATA_FILE) as f:
                districts = json.load(f)
            for d in districts:
                if d['name'] == district:
                    d['status'] = 'engaged'
                    break
            with open(DATA_FILE, 'w') as f:
                json.dump(districts, f, indent=2, ensure_ascii=False)

            # Send Telegram alert
            alert = f"REPLY from {name} at {district}!"
            send_telegram(alert)

            # Log reply
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message_id": msg_id,
                "sender": sender,
                "district": district,
                "contact_name": name,
                "hubspot_contact_id": contact_id,
                "hubspot_deal_id": deal_id
            }
            with open(REPLY_LOG, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')

    summary = f"Reply check complete: {new_replies} new replies detected"
    print(f"\n{summary}")

    if new_replies > 0:
        send_telegram(f"ModelIt Reply Check: {new_replies} new replies!")
    else:
        # Only report "no replies" during business hours
        hour = datetime.now().hour
        if hour in (12, 18):  # Noon and 6 PM only
            send_telegram("ModelIt Reply Check: No new replies")


if __name__ == "__main__":
    main()
