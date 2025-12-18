from datetime import datetime
from fnmatch import fnmatch
from glob import glob
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from flask import Flask, jsonify, render_template, request
import paramiko

import configparser

from database import initialize_db, insert_log_entry, query_logs, sqlite_is_empty, should_ingest_file, mark_file_ingested

config = configparser.ConfigParser()
config.read('config.ini')

# ---- Local fallback paths ----

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

LOCAL_SYNC_DIR = Path(config.get('Default', 'local_sync_dir', fallback='synced_logs'))

LOCAL_GLOB_PATTERN = str(LOCAL_SYNC_DIR / "dc-*" / "application*")
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
    """Return remote files with full metadata: name, path, size, mtime."""
    results = []
    try:
        for entry in sftp.listdir_attr(REMOTE_BASE_DIR):
            if not fnmatch(entry.filename, REMOTE_SUBDIR_PATTERN):
                continue
            subdir = f"{REMOTE_BASE_DIR}/{entry.filename}"
            for f in sftp.listdir_attr(subdir):
                if fnmatch(f.filename, REMOTE_FILE_PATTERN):
                    results.append({
                        "remote_path": f"{subdir}/{f.filename}",
                        "filename": f"{entry.filename}/{f.filename}",
                        "size": f.st_size,
                        "mtime": f.st_mtime,
                    })
    except Exception as exc:
        print(f"Remote listing failed: {exc}")
    return results

def local_file_metadata(local_path: Path) -> Optional[Dict]:
    if not local_path.exists():
        return None
    return {
        "size": local_path.stat().st_size,
        "mtime": int(local_path.stat().st_mtime),
    }

def sync_remote_to_local() -> List[Path]:
    print("=== SYNC START ===")
    downloaded_files = []
    sftp = client = None

    try:
        sftp, client = sftp_client()
        print("Connected to SFTP.")

        remote_files = list_remote_paths(sftp)
        print(f"Found {len(remote_files)} remote files.")

        for rf in remote_files:
            rel_path = Path(rf["filename"])
            local_path = LOCAL_SYNC_DIR / rel_path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            local_meta = local_file_metadata(local_path)

            needs_download = (
                local_meta is None or
                local_meta["size"] != rf["size"] or
                rf["mtime"] > local_meta["mtime"]
            )

            if needs_download:
                print(f"Downloading {rf['remote_path']} → {local_path}")
                with sftp.file(rf["remote_path"], "r") as remote_f:
                    content = remote_f.read().decode("utf-8", errors="replace")

                local_path.write_text(content, encoding="utf-8")
                downloaded_files.append(local_path)

            else:
                print(f"Skipping unchanged file: {local_path}")
                content = None  # IMPORTANT

            # ---------- INGESTION DECISION ----------
            stat = local_path.stat()
            file_key = str(local_path.resolve())

            if should_ingest_file(file_key, stat.st_size, int(stat.st_mtime)):
                print(f"Ingesting {local_path}")
                if content is None:
                    content = local_path.read_text(encoding="utf-8", errors="replace")

                entries = parse_blocks(content, file_key)
                ingest_entries(entries)
                mark_file_ingested(file_key, stat.st_size, int(stat.st_mtime))
            else:
                print(f"Already ingested, skipping parse: {local_path}")

    except Exception as exc:
        print(f"SYNC ERROR: {exc}")

    finally:
        if sftp:
            sftp.close()
        if client:
            client.close()

    print(f"=== SYNC DONE: {len(downloaded_files)} files downloaded ===")
    return downloaded_files

def parse_blocks(raw_content: str,remote_path: str) -> List[Dict[str, str]]:
    parsed = []
    for block in raw_content.split(LOG_SEPARATOR):
        entry = parse_log_block(block,remote_path)
        if entry:
            parsed.append(entry)
            #insert_log_entry(entry)
            #parsed.append(entry)
    return parsed

def ingest_entries(entries: list[dict]):
    for entry in entries:
        insert_log_entry(entry)


def parse_log_block(block: str, remote_path: str) -> Optional[Dict[str, str]]:
    cleaned = block.strip()
    if not cleaned:
        return None

    fields = []
    remainder = cleaned

    for _ in range(5):
        start = remainder.find("[")
        end = remainder.find("]")
        if start == -1 or end == -1 or end < start:
            return None

        fields.append(remainder[start+1:end].strip())
        remainder = remainder[end+1:].strip()
    if len(fields) != 5:
        return None

    log_type, collector, level, pool, source = fields

    date, time, message = extract_date_time_and_message(remainder)
    dt = build_datetime(date, time)

    filename = remote_path.split("/")[-1]
    return {
        "type": log_type,
        "collector": collector+" (" + filename + ")",
        "level": level,
        "pool": pool,
        "source": source,
        "date": date,
        "time": time,
        "message": message,
        "datetime": dt.isoformat() if dt else None,
        "filename": filename,
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


def load_all_logs() -> List[Dict[str, str]]:
    raise RuntimeError("Depricated")  # Placeholder for loading logs from DB if needed


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
    ##logs = load_all_logs()
    return render_template("index.html", logs=[])

@app.route("/sync")
def sync_route():

    updated = sync_remote_to_local()
    return jsonify({"updated": len(updated)})

@app.route("/ingest/local", methods=["POST"])
def ingest_local():
    indexed = 0
    skipped = 0
    # if we put a file manually in the synced logs dir, this will fetch it in the DB
    files = list(LOCAL_SYNC_DIR.rglob("*.log"))

    for path in LOCAL_SYNC_DIR.rglob("*.log"):
        stat = path.stat()
        file_key = str(path.resolve())

        if not should_ingest_file(file_key, stat.st_size, int(stat.st_mtime)):
            skipped += 1
            print(f"skipped {file_key} already exists")
            continue

        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
            print("Parsing file: ", file_key)
            entries = parse_blocks(raw,file_key)  ##put in db
            ingest_entries(entries)
            mark_file_ingested(file_key, stat.st_size, int(stat.st_mtime))
            indexed += 1
        except Exception as exc:
            print(f"Failed to ingest file: {file}: {exec}")
    return jsonify({"indexed_files":indexed, "skipped": skipped})

@app.route("/api/logs")
def api_logs():
        # Pagination
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 200))
    offset = (page - 1) * limit

    # Filters
    search = request.args.get("search")
    type_f = request.args.get("type")
    collector_f = request.args.get("collector")
    level_f = request.args.get("level")
    pool_f = request.args.get("pool")
    source_f = request.args.get("source")

    # Date/time
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    start_dt = None
    end_dt = None

    try:
        if start_date and start_time:
            start_dt = int(datetime.fromisoformat(f"{start_date} {start_time}").timestamp())
        elif start_date:
            start_dt = int(datetime.fromisoformat(f"{start_date} 00:00").timestamp())

        if end_date and end_time:
            end_dt = int(datetime.fromisoformat(f"{end_date} {end_time}").timestamp())
        elif end_date:
            end_dt = int(datetime.fromisoformat(f"{end_date} 23:59").timestamp())
    except:
        pass

    # Sorting
    sort_by = request.args.get("sort_by", "datetime")
    direction = request.args.get("direction", "desc")

    rows, total = query_logs(
        search=search,
        type_filter=type_f,
        collector_filter=collector_f,
        level_filter=level_f,
        pool_filter=pool_f,
        source_filter=source_f,
        start_date=start_dt,
        end_date=end_dt,
        sort_by=sort_by,
        direction=direction,
        limit=limit,
        offset=offset,
    )

    return jsonify({
        "page": page,
        "limit": limit,
        "returned": len(rows),
        "total": total,
        "logs": rows
    })

#@app.route("/test")
#def test_route():
#    return str(read_remote_logs()), 200

if __name__ == "__main__":
    print("init database")
    initialize_db()
    print("Remote enabled" if USE_REMOTE else "Using local logs")
    print("Local matches:", collect_local_files())
    app.run(host="0.0.0.0", port=5000, debug=False)
