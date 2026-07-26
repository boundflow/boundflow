import json
import os
import urllib.request

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")


def notify_approval_requested(req):
    if not SLACK_WEBHOOK_URL:
        return
    text = (
        f":rotating_light: Approval needed on workflow `{req.workflow_id[:8]}`\n"
        f"> {req.justification}\n"
        f"`boundflow workflow approve {req.workflow_id} {req.approval_id}`\n"
        f"`boundflow workflow reject {req.workflow_id} {req.approval_id}`"
    )
    body = json.dumps({"text": text}).encode()
    request = urllib.request.Request(
        SLACK_WEBHOOK_URL, data=body, headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(request, timeout=5)
