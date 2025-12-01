from datetime import datetime
from fnmatch import fnmatch
from glob import glob
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from flask import Flask, jsonify, render_template, request
import paramiko

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# ---- Local fallback paths ----
LOCAL_GLOB_PATTERN = "/app1/logs/dc-*/application*"
FALLBACK_LOG = Path("input") / "sample.log"
LOG_SEPARATOR = "$$$"

# ---- Remote SFTP config ----
USE_REMOTE = True  # set False to read only local files
REMOTE_HOST = config.get('Default', 'Host', fallback='localhost')
REMOTE_PORT = config.getint('Default', 'remote_port', fallback=22)
REMOTE_USER = config.get('Default', 'Username', fallback='eventum')
REMOTE_PASSWORD = config.get('Default', 'Password', fallback='P@ssw0rd')  # or set to password string
REMOTE_BASE_DIR = config.get('Default', 'remote_path', fallback='/app1/logs')
REMOTE_SUBDIR_PATTERN = config.get('Default', 'remote_subdir', fallback='dc-snmp')
REMOTE_FILE_PATTERN = config.get('Default', 'remote_file_pattern', fallback='application*')

app = Flask(__name__)


def collect_local_files() -> List[Path]: #local files path >> not sftp
    files = [Path(p) for p in glob(LOCAL_GLOB_PATTERN) if Path(p).exists()]
    if not files and FALLBACK_LOG.exists():
        files = [FALLBACK_LOG]
    return files


def sftp_client():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        REMOTE_HOST,
        port=REMOTE_PORT,
        username=REMOTE_USER,
        password=REMOTE_PASSWORD,
        timeout=3,
        banner_timeout=3,
        auth_timeout=3,
    )
    return client.open_sftp(), client


def list_remote_paths(sftp) -> List[str]:
    """List remote files matching base/subdir/file patterns via SFTP (no shell globbing)."""
    matches: List[str] = []
    try:
        for entry in sftp.listdir_attr(REMOTE_BASE_DIR):
            if not fnmatch(entry.filename, REMOTE_SUBDIR_PATTERN):
                continue
            subdir = f"{REMOTE_BASE_DIR}/{entry.filename}"
            for file_entry in sftp.listdir_attr(subdir):
                if fnmatch(file_entry.filename, REMOTE_FILE_PATTERN):
                    matches.append(f"{subdir}/{file_entry.filename}")
    except Exception as exc:  # noqa: BLE001
        print(f"Remote listing failed: {exc}")
    return matches


def read_remote_logs() -> List[Dict[str, str]]:
    print("read_remote_logs(): START")
    logs: List[Dict[str, str]] = []
    sftp = client = None

    try:
        print("Creating SFTP client...")
        sftp, client = sftp_client()
        print("SFTP client created.")

        print("Listing remote paths...")
        remote_paths = list_remote_paths(sftp)  ##list files in the remote path
        print(f"Found {len(remote_paths)} remote paths: {remote_paths}")

        for remote_path in remote_paths:
            print(f"Reading remote file: {remote_path}")
            try:
                with sftp.file(remote_path, "r") as f:
                    print(f"Opened remote file: {remote_path}, reading...")
                    raw_content = f.read().decode("utf-8", errors="replace")
                    print(f"Read {len(raw_content)} bytes from {remote_path}")

                    print("Parsing blocks...")
                    parsed = parse_blocks(raw_content,remote_path)
                    print(f"Parsed {len(parsed)} log entries from {remote_path}")

                    logs.extend(parsed)

            except Exception as exc:
                print(f"Failed reading {remote_path}: {exc}")

    except Exception as exc:
        print(f"SFTP connection failed: {exc}")

    finally:
        print("Closing SFTP and SSH...")
        if sftp:
            sftp.close()
        if client:
            client.close()

    print(f"read_remote_logs(): DONE, total logs={len(logs)}")
    return logs


def parse_blocks(raw_content: str,remote_path: str) -> List[Dict[str, str]]:
    parsed: List[Dict[str, str]] = []
    for block in raw_content.split(LOG_SEPARATOR):
        entry = parse_log_block(block,remote_path)
        if entry:
            parsed.append(entry)
    return parsed


def parse_log_block(block: str, remote_path: str) -> Optional[Dict[str, str]]:
    cleaned = block.strip()
    if not cleaned:
        return None

    parts = [segment.replace("[", "").strip() for segment in cleaned.split("]") if segment.strip()]
    if len(parts) < 6:
        return None

    log_type, collector, level, pool, source = parts[:5]
    remainder = parts[5]

    date, time, message = extract_date_time_and_message(remainder)
    dt = build_datetime(date, time)

    return {
        "type": log_type,
        "collector": collector+" (" + remote_path.split("/")[-1] + ")",
        "level": level,
        "pool": pool,
        "source": source,
        "date": date,
        "time": time,
        "message": message,
        "datetime": dt.isoformat() if dt else None,
    }


def extract_date_time_and_message(remainder: str) -> Tuple[str, str, str]:
    segments = remainder.strip().split()
    if len(segments) < 2:
        return "", "", remainder.strip()
    date, time = segments[0], segments[1]
    message = remainder[remainder.find(time) + len(time):].strip()
    return date, time, message


def build_datetime(date_value: str, time_value: str) -> Optional[datetime]:
    if not date_value or not time_value:
        return None
    try:
        return datetime.fromisoformat(f"{date_value} {time_value}")
    except ValueError:
        return None


def read_local_logs_from_files(paths: List[Path]) -> List[Dict[str, str]]:
    logs: List[Dict[str, str]] = []
    for path in paths:
        try:
            raw_content = path.read_text(encoding="utf-8", errors="replace")
            logs.extend(parse_blocks(raw_content))
        except Exception as exc:  # noqa: BLE001
            print(f"Failed reading {path}: {exc}")
    return logs


def load_all_logs() -> List[Dict[str, str]]:
    if USE_REMOTE:
        logs = read_remote_logs()
        if logs:
            return logs
        print("Remote fetch empty/failed, falling back to local.")
    return read_local_logs_from_files(collect_local_files())


def apply_filters(logs: List[Dict[str, str]], args) -> List[Dict[str, str]]:
    search_term = (args.get("search") or "").lower()
    type_filter = (args.get("type") or "").lower()
    collector_filter = (args.get("collector") or "").lower()
    level_filter = (args.get("level") or "").lower()
    pool_filter = (args.get("pool") or "").lower()
    source_filter = (args.get("source") or "").lower()

    start_date = args.get("start_date")
    end_date = args.get("end_date")
    start_time = args.get("start_time")
    end_time = args.get("end_time")

    start_date_val = datetime.fromisoformat(start_date).date() if start_date else None
    end_date_val = datetime.fromisoformat(end_date).date() if end_date else None
    start_time_val = datetime.strptime(start_time, "%H:%M").time() if start_time else None
    end_time_val = datetime.strptime(end_time, "%H:%M").time() if end_time else None

    filtered: List[Dict[str, str]] = []
    for log in logs:
        search_haystack = " ".join(str(v or "") for v in log.values()).lower()
        if search_term and search_term not in search_haystack:
            continue
        if type_filter and log.get("type", "").lower() != type_filter:
            continue
        if collector_filter and log.get("collector", "").lower() != collector_filter:
            continue
        if level_filter and log.get("level", "").lower() != level_filter:
            continue
        if pool_filter and log.get("pool", "").lower() != pool_filter:
            continue
        if source_filter and log.get("source", "").lower() != source_filter:
            continue

        dt = build_datetime(log.get("date", ""), log.get("time", ""))
        if dt and start_date_val and dt.date() < start_date_val:
            continue
        if dt and end_date_val and dt.date() > end_date_val:
            continue
        if dt and start_time_val and dt.time().replace(tzinfo=None) < start_time_val:
            continue
        if dt and end_time_val and dt.time().replace(tzinfo=None) > end_time_val:
            continue

        filtered.append(log)

    return filtered


def apply_sort(logs: List[Dict[str, str]], sort_by: str, direction: str) -> List[Dict[str, str]]:
    sort_field = sort_by or "datetime"
    reverse = (direction or "desc").lower() == "desc"

    def sort_key(log: Dict[str, str]):
        if sort_field == "datetime":
            dt = build_datetime(log.get("date", ""), log.get("time", ""))
            return dt.timestamp() if dt else float("-inf")
        return log.get(sort_field, "")

    return sorted(logs, key=sort_key, reverse=reverse)


@app.route("/")
def index():
    logs = load_all_logs()
    return render_template("index.html", logs=logs)


@app.route("/api/logs")
def api_logs():
    logs = load_all_logs()
    logs = apply_filters(logs, request.args)
    logs = apply_sort(logs, request.args.get("sort_by", "datetime"), request.args.get("direction", "desc"))
    return jsonify({"count": len(logs), "logs": logs})

@app.route("/test")
def test_route():
    return str(read_remote_logs()), 200

if __name__ == "__main__":
    print("Remote enabled" if USE_REMOTE else "Using local logs")
    print("Local matches:", collect_local_files())
    app.run(host="0.0.0.0", port=5000, debug=False)
