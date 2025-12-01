from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from flask import Flask, jsonify, render_template, request
from glob import glob

LOG_FILES = [Path(p) for p in glob("/app1/logs/dc-*/application*")]
LOG_SEPARATOR = "$$$"

app = Flask(__name__)


def parse_log_block(block: str) -> Optional[Dict[str, str]]:
    """
    Parse a raw log block into structured fields.
    Returns None when the block does not match the expected layout.
    """
    cleaned = block.strip()
    if not cleaned:
        return None

    # Split by closing bracket to separate the first five fields.
    parts = [segment.replace("[", "").strip() for segment in cleaned.split("]") if segment.strip()]
    if len(parts) < 6:
        return None

    log_type, collector, level, pool, source = parts[:5]
    remainder = parts[5]

    date, time, message = extract_date_time_and_message(remainder)
    dt = build_datetime(date, time)

    return {
        "type": log_type,
        "collector": collector,
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
    # Strip date + time and the spaces separating them to keep the full log text intact.
    message = remainder[remainder.find(time) + len(time):].strip()
    return date, time, message


def build_datetime(date_value: str, time_value: str) -> Optional[datetime]:
    if not date_value or not time_value:
        return None
    try:
        return datetime.fromisoformat(f"{date_value} {time_value}")
    except ValueError:
        return None


def read_logs(LOG_FILE) -> List[Dict[str, str]]:
    if not LOG_FILE.exists():
        return []

    raw_content = LOG_FILE.read_text(encoding="utf-8")
    blocks = raw_content.split(LOG_SEPARATOR)
    parsed = []
    for block in blocks:
        entry = parse_log_block(block)
        if entry:
            parsed.append(entry)
    return parsed


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
        if search_term and search_term not in " ".join(log.values()).lower():
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
    logs = []
    for log_file in LOG_FILES:
        logs.extend(read_logs(log_file))
    return render_template("index.html", logs=logs)


@app.route("/api/logs")
def api_logs():
    logs = []
    for log_file in LOG_FILES:
        logs.extend(read_logs(log_file))
    logs = apply_filters(logs, request.args)
    logs = apply_sort(logs, request.args.get("sort_by", "datetime"), request.args.get("direction", "desc"))
    return jsonify({"count": len(logs), "logs": logs})


if __name__ == "__main__":
    print(LOG_FILES)
    app.run(host="0.0.0.0", port=5000, debug=True)
