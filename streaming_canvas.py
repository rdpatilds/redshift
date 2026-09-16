# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.34"]
# ///
"""Point the local Canvas checkout's Live Events client at the live Kinesis stream, then
restart the containers that read config at boot.

DynamicSettings resolves `live_events.yml` from the `private` tree
(config/initializers/live_events.rb reads `DynamicSettings.find(tree: :private)`), not the
`config` tree the checkout ships a stub under, so this edits development.private.canvas and
leaves the stale development.config.canvas block alone.

Run:  uv run streaming_canvas.py [--restart]
"""
import argparse
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.request

from streaming import KEY_FILE, NAME, REGION

CPLATFORM_DIR = pathlib.Path(r"D:\Canvas\test\canvas\cplatform")
DYNAMIC_SETTINGS = CPLATFORM_DIR / "config" / "dynamic_settings.yml"
CONTAINERS = ["cplatform-web-1", "cplatform-jobs-1"]
CANVAS_URL = "http://localhost:3100"
CANVAS_TOKEN = "cplatform-dev-token"
KEY_INDENT = "      "
BODY_INDENT = "        "
LIVE_EVENTS_KEY = f"{KEY_INDENT}live_events.yml:"


def read_producer_keys():
    values = dict(line.split("=", 1) for line in KEY_FILE.read_text().splitlines() if line.strip())
    return values["AWS_ACCESS_KEY_ID"], values["AWS_SECRET_ACCESS_KEY"]


def desired_block(access_key_id, secret_access_key):
    return [
        f"{LIVE_EVENTS_KEY} |-\n",
        f"{BODY_INDENT}kinesis_stream_name: {NAME}\n",
        f"{BODY_INDENT}aws_region: {REGION}\n",
        f"{BODY_INDENT}aws_access_key_id: {access_key_id}\n",
        f"{BODY_INDENT}aws_secret_access_key_dec: {secret_access_key}\n",
    ]


def indent_of(line):
    return len(line) - len(line.lstrip(" "))


def find_private_canvas(lines):
    private_at = next(i for i, l in enumerate(lines) if l.rstrip("\n") == "  private:")
    canvas_at = next(i for i in range(private_at + 1, len(lines)) if lines[i].rstrip("\n") == "    canvas:")
    end = len(lines)
    for i in range(canvas_at + 1, len(lines)):
        if lines[i].strip() and indent_of(lines[i]) <= 4:
            end = i
            break
    return canvas_at, end


def splice_live_events(lines, block):
    canvas_at, canvas_end = find_private_canvas(lines)
    key_at = next((i for i in range(canvas_at + 1, canvas_end) if lines[i].startswith(LIVE_EVENTS_KEY)), None)
    if key_at is None:
        return lines[:canvas_end] + block + lines[canvas_end:], True
    value_end = key_at + 1
    while value_end < canvas_end and (not lines[value_end].strip() or indent_of(lines[value_end]) >= len(BODY_INDENT)):
        value_end += 1
    if lines[key_at:value_end] == block:
        return lines, False
    return lines[:key_at] + block + lines[value_end:], True


def wait_for_canvas(timeout=300, interval=5):
    request = urllib.request.Request(f"{CANVAS_URL}/api/v1/users/self",
                                      headers={"Authorization": f"Bearer {CANVAS_TOKEN}"})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status == 200:
                    print(f"{CANVAS_URL}/api/v1/users/self answered 200")
                    return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(interval)
    sys.exit(f"Canvas did not answer 200 at {CANVAS_URL} within {timeout}s of restart")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--restart", action="store_true", help="restart the containers even if the file is unchanged")
    args = parser.parse_args()

    access_key_id, secret_access_key = read_producer_keys()
    lines = DYNAMIC_SETTINGS.read_text().splitlines(keepends=True)
    new_lines, changed = splice_live_events(lines, desired_block(access_key_id, secret_access_key))
    if changed:
        DYNAMIC_SETTINGS.write_text("".join(new_lines))
        print(f"wrote development.private.canvas['live_events.yml'] in {DYNAMIC_SETTINGS}")
    else:
        print("development.private.canvas['live_events.yml'] already matches, leaving the file alone")

    if not (changed or args.restart):
        print("no restart needed")
        return

    subprocess.run(["docker", "restart", *CONTAINERS], check=True)
    print(f"restarted {', '.join(CONTAINERS)}")
    wait_for_canvas()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
