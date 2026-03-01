#!/usr/bin/env python3
"""
ModelIt Daily Digest — Aggregates stats and sends Telegram summary at 8 PM PT.
"""

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = REPO_DIR / "data" / "cde-districts.json"
OUTREACH_LOG = REPO_DIR / "data" / "outreach-log.jsonl"
REPLY_LOG = REPO_DIR / "data" / "reply-log.jsonl"
RESEARCH_LOG = REPO_DIR / "data" / "research-log.jsonl"

TELEGRAM_GROUP = os.environ.get("TELEGRAM_GROUP", "-5163496634")


def send_telegram(message):
    """Send Telegram notification."""
    cmd = f'openclaw send --to "{TELEGRAM_GROUP}" --message "{message}"'
    subprocess.run(cmd, shell=True, capture_output=True)


def count_log_today(log_file, status_filter=None):
    """Count entries logged today."""
    if not log_file.exists():
        return 0
    today = datetime.now().strftime("%Y-%m-%d")
    count = 0
    with open(log_file) as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                ts = entry.get("timestamp", "")
                if today in ts:
                    if status_filter is None or entry.get("status") == status_filter:
                        count += 1
            except json.JSONDecodeError:
                continue
    return count


def count_log_total(log_file, status_filter=None):
    """Count all entries in a log file."""
    if not log_file.exists():
        return 0
    count = 0
    with open(log_file) as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                if status_filter is None or entry.get("status") == status_filter:
                    count += 1
            except json.JSONDecodeError:
                continue
    return count


def get_next_batch_names(districts, count=5):
    """Get names of next districts to be researched."""
    unresearched = [d for d in districts if d['status'] == 'unresearched']
    return [d['name'] for d in unresearched[:count]]


def main():
    today = datetime.now().strftime("%b %-d, %Y")

    # Load districts
    with open(DATA_FILE) as f:
        districts = json.load(f)

    total_districts = len(districts)
    researched = sum(1 for d in districts if d['status'] != 'unresearched')
    contacted = sum(1 for d in districts if d['status'] in ('contacted', 'engaged'))
    engaged = sum(1 for d in districts if d['status'] == 'engaged')
    unresearched = sum(1 for d in districts if d['status'] == 'unresearched')

    # Today's stats
    researched_today = count_log_today(RESEARCH_LOG, "success")
    emails_today = count_log_today(OUTREACH_LOG, "sent")
    replies_today = count_log_today(REPLY_LOG)

    # Totals
    total_emails = count_log_total(OUTREACH_LOG, "sent")
    total_replies = count_log_total(REPLY_LOG)

    # Pipeline estimate (rough: $10K-$50K per district deal)
    pipeline_low = contacted * 10000
    pipeline_high = contacted * 50000

    # Next batch
    next_names = get_next_batch_names(districts, 5)

    # Format Telegram message
    message = f"""ModelIt Daily Digest — {today}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Research: {researched_today} districts profiled today ({researched}/{total_districts} total)
Outreach: {emails_today} emails sent today ({total_emails} total sent)
Replies: {replies_today} new today ({total_replies} total, {engaged} engaged)
Pipeline: ${pipeline_low:,}-${pipeline_high:,} across {contacted} deals
Remaining: {unresearched} districts unresearched
Next batch: {', '.join(next_names[:3])}...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

    print(message)
    send_telegram(message)


if __name__ == "__main__":
    main()
