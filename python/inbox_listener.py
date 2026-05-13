"""Inbox listener — polls IMAP for task emails and submits to pipeline.

Runs as a background thread inside the SDK process. Each poll:
1. Connect to IMAP INBOX
2. Search for UNSEEN messages matching a subject prefix
3. Parse subject → project_path, body → prompt
4. POST to /api/submit
5. Mark messages as SEEN

Also exposes a Mailgun webhook endpoint as an alternative — no polling needed
when Mailgun forwards inbound emails.
"""
from __future__ import annotations

import email
import email.policy
import imaplib
import json
import logging
import re
import threading
import time
import urllib.request
import urllib.error
from typing import Any

from pipeline_config import (
    SDK_URL, INBOX_POLL_INTERVAL,
    INBOX_IMAP_HOST, INBOX_IMAP_USER, INBOX_IMAP_PASS,
)

log = logging.getLogger("inbox")

# Subject prefix to identify pipeline tasks
TASK_SUBJECT_PREFIX = "[pipeline]"
# Subject format: [pipeline] /path/to/project
# Body: the actual task prompt

SUBJECT_RE = re.compile(r"^\[pipeline\]\s*(.+)$", re.IGNORECASE)


def _parse_message(msg_bytes: bytes) -> dict[str, str] | None:
    """Parse raw email bytes into {subject, from, body, project_path}."""
    msg = email.message_from_bytes(msg_bytes, policy=email.policy.default)
    subject = str(msg.get("Subject", "")).strip()
    from_addr = str(msg.get("From", ""))

    # Extract body (prefer text/plain)
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="replace")
                    break
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            body = payload.decode("utf-8", errors="replace")

    match = SUBJECT_RE.match(subject)
    if not match:
        return None

    project_path = match.group(1).strip()
    return {
        "subject": subject,
        "from": from_addr,
        "body": body.strip(),
        "project_path": project_path or "/var/www/kraken/Dashboard",
    }


def _submit_task(parsed: dict) -> bool:
    """POST to SDK /api/submit."""
    payload = {
        "prompt": parsed["body"],
        "project_path": parsed["project_path"],
        "source": "inbox",
    }
    data = json.dumps(payload).encode("utf-8")
    url = f"{SDK_URL}/api/submit"
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except urllib.error.URLError as e:
        log.error("Submit failed: %s", e)
        return False


def _poll_once() -> int:
    """One IMAP poll cycle. Returns number of tasks submitted."""
    if not all([INBOX_IMAP_HOST, INBOX_IMAP_USER, INBOX_IMAP_PASS]):
        return 0

    submitted = 0
    try:
        imap = imaplib.IMAP4_SSL(INBOX_IMAP_HOST)
        imap.login(INBOX_IMAP_USER, INBOX_IMAP_PASS)
        imap.select("INBOX")

        # Search unseen
        status, msg_ids = imap.search(None, "UNSEEN")
        if status != "OK":
            return 0

        ids = msg_ids[0].split()
        for mid in ids:
            status, data = imap.fetch(mid, "(RFC822)")
            if status != "OK":
                continue
            raw = data[0][1]
            parsed = _parse_message(raw)
            if parsed:
                ok = _submit_task(parsed)
                if ok:
                    log.info("Submitted task from %s: %.80s", parsed["from"], parsed["body"])
                    submitted += 1
            # Mark as seen regardless
            imap.store(mid, "+FLAGS", "\\Seen")

        imap.logout()
    except imaplib.IMAP4.error as e:
        log.error("IMAP error: %s", e)
    except Exception as e:
        log.error("Poll error: %s", e)

    return submitted


def _inbox_loop() -> None:
    """Background thread — polls IMAP every N seconds."""
    log.info("Inbox listener started (interval=%ds)", INBOX_POLL_INTERVAL)
    while True:
        try:
            n = _poll_once()
            if n:
                log.info("Inbox: submitted %d tasks", n)
        except Exception as e:
            log.error("Inbox loop error: %s", e)
        time.sleep(INBOX_POLL_INTERVAL)


def start_inbox_listener() -> threading.Thread:
    """Start the inbox listener in a daemon thread."""
    if not all([INBOX_IMAP_HOST, INBOX_IMAP_USER, INBOX_IMAP_PASS]):
        log.info("IMAP not configured, inbox listener disabled")
        t = threading.Thread(target=lambda: None, daemon=True)
        t.start()
        return t

    t = threading.Thread(target=_inbox_loop, name="inbox-listener", daemon=True)
    t.start()
    return t


# ─── Mailgun webhook handler (FastAPI route) ────────────────────────────

def handle_mailgun_webhook(body: dict) -> dict:
    """Parse Mailgun inbound webhook and submit task directly to DB.

    Writes to DB directly instead of HTTP round-trip to avoid
    single-thread deadlock in uvicorn.
    """
    subject = body.get("subject", "")
    match = SUBJECT_RE.match(subject)
    if not match:
        return {"status": "ignored", "reason": "subject prefix not matched"}

    project_path = match.group(1).strip() or "/var/www/kraken/Dashboard"
    prompt = body.get("body-plain", "").strip()
    sender = body.get("sender", "")

    if not prompt:
        return {"status": "ignored", "reason": "empty body"}

    import agent_db
    flow_id = agent_db.create_task_flow(
        prompt=prompt,
        project_path=project_path,
        source="inbox",
        metadata={"sender": sender, "subject": subject},
    )
    return {"status": "submitted", "flow_id": flow_id, "from": sender}
