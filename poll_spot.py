"""
poll_spot.py

Fetches the latest positions from a SPOT Trace XML feed and pushes each
new observation to Gundi's observations endpoint.

Required environment variables (set as GitHub Secrets, injected by the
workflow):
  SPOT_FEED_URL   - the full SPOT XML feed URL (from SPOT's "XML Feed" page)
  GUNDI_API_KEY   - the API key shown on the Gundi Connection's "API key" tab
  GUNDI_ENDPOINT  - Gundi observations endpoint
                    (default: https://sensors.api.gundiservice.org/v2/observations/)

State:
  To avoid re-sending the same position on every run, this script keeps a
  small JSON file (last_seen.json) in the repo recording the timestamp of
  the last message it already sent, and commits it back via the workflow.
"""

import os
import sys
import json
import datetime
import xml.etree.ElementTree as ET

import requests

STATE_FILE = "last_seen.json"


def load_last_seen():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f).get("last_seen_utc")
    return None


def save_last_seen(timestamp_utc):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_seen_utc": timestamp_utc}, f)


def fetch_spot_feed(feed_url):
    resp = requests.get(feed_url, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_messages(xml_text):
    """
    Parses SPOT's XML feed and returns a list of dicts:
    {device_id, latitude, longitude, timestamp_utc, message_type}
    Skips entries missing lat/lon (e.g. some non-location message types).
    """
    root = ET.fromstring(xml_text)
    messages = []

    # SPOT's feed nests messages under feedMessageResponse/messages/message
    for message in root.iter("message"):
        def get_text(tag):
            el = message.find(tag)
            return el.text if el is not None else None

        lat = get_text("latitude")
        lon = get_text("longitude")
        dt = get_text("dateTime")
        device_id = get_text("messengerId") or get_text("esn")
        msg_type = get_text("messageType")

        if lat is None or lon is None or dt is None or device_id is None:
            continue

        messages.append(
            {
                "device_id": device_id,
                "latitude": float(lat),
                "longitude": float(lon),
                "timestamp_utc": dt,
                "message_type": msg_type,
            }
        )

    # Oldest first, so we send in chronological order
    messages.sort(key=lambda m: m["timestamp_utc"])
    return messages


def to_gundi_payload(message):
    return {
        "source": message["device_id"],
        "recorded_at": message["timestamp_utc"],
        "location": {"lat": message["latitude"], "lon": message["longitude"]},
        "additional": {"message_type": message["message_type"]},
    }


def push_to_gundi(endpoint, api_key, payload):
    resp = requests.post(
        endpoint,
        headers={"apikey": api_key},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp


def main():
    feed_url = os.environ.get("SPOT_FEED_URL")
    api_key = os.environ.get("GUNDI_API_KEY")
    endpoint = os.environ.get(
        "GUNDI_ENDPOINT", "https://sensors.api.gundiservice.org/v2/observations/"
    )

    if not feed_url or not api_key:
        print("ERROR: SPOT_FEED_URL and GUNDI_API_KEY must be set.", file=sys.stderr)
        sys.exit(1)

    xml_text = fetch_spot_feed(feed_url)
    messages = parse_messages(xml_text)

    last_seen = load_last_seen()
    new_messages = [m for m in messages if not last_seen or m["timestamp_utc"] > last_seen]

    if not new_messages:
        print(f"[{datetime.datetime.utcnow().isoformat()}] No new positions.")
        return

    sent = 0
    for m in new_messages:
        payload = to_gundi_payload(m)
        try:
            push_to_gundi(endpoint, api_key, payload)
            sent += 1
            save_last_seen(m["timestamp_utc"])
        except requests.HTTPError as e:
            print(
                f"ERROR pushing message at {m['timestamp_utc']}: "
                f"{e.response.status_code} {e.response.text}",
                file=sys.stderr,
            )
            # Stop on first failure so we retry it (and any after it) next run
            break

    print(
        f"[{datetime.datetime.utcnow().isoformat()}] Sent {sent}/{len(new_messages)} "
        f"new position(s) to Gundi."
    )


if __name__ == "__main__":
    main()
