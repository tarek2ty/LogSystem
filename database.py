import sqlite3
from pathlib import Path

DB_PATH = Path("logs.db")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            collector TEXT NOT NULL,
            level TEXT NOT NULL,
            pool TEXT,
            source TEXT,
            date  TEXT NOT NULL,
            time TEXT NOT NULL,
            datetime TEXT NOT NULL,
            filename TEXT NOT NULL,
            message TEXT NOT NULL,
                   
            UNIQUE(message, datetime, filename)
        )
    ''')
    cursor.execute('''
       CREATE TABLE IF NOT EXISTS file_ingest_state (
           file_path TEXT PRIMARY KEY,
           size INTEGER NOT NULL,
           mtime INTEGER NOT NULL,
           last_ingested_at TEXT NOT NULL
       )
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_ingest_mtime ON file_ingest_state (mtime)')
    cursor.execute('Create INDEX IF NOT EXISTS idx_logs_datetime ON logs (datetime)')
    cursor.execute('Create INDEX IF NOT EXISTS idx_logs_type ON logs (type)')
    cursor.execute('Create INDEX IF NOT EXISTS idx_logs_collector ON logs (collector)')
    cursor.execute('Create INDEX IF NOT EXISTS idx_logs_level ON logs (level)')


    conn.commit()
    conn.close()

def insert_log_entry(log_entry):
    conn = get_db_connection()
    cursor = conn.cursor()
    ##insert or ignore to avoid duplicates
    cursor.execute('''
        INSERT OR IGNORE INTO logs 
        (type, collector, level, pool, source, date, time, datetime, filename, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        log_entry['type'],
        log_entry['collector'],
        log_entry['level'],
        log_entry.get('pool'),
        log_entry.get('source'),
        log_entry['date'],
        log_entry['time'],
        log_entry['datetime'],
        log_entry['filename'],
        log_entry['message']
    ))
    conn.commit()
    conn.close()

def query_logs(
        search=None,
        type_filter=None,
        collector_filter=None,
        level_filter=None,
        pool_filter=None,
        source_filter=None,
        start_date=None,
        end_date=None,
        sort_by='datetime',
        direction='DESC',
        limit=100,
        offset=0,
    ):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM logs WHERE 1=1"
    params = []

    if search:
        query += " AND (message LIKE ? or type LIKE ? or collector LIKE ? or level LIKE ? or pool LIKE ? or source LIKE ?)"
        search_param = f"%{search}%"
        params.extend([search_param] * 6)

    if type_filter:
        query += " AND type = ?"
        params.append(type_filter)

    if collector_filter:
        query += " AND collector = ?"
        params.append(collector_filter)
    if level_filter:
        query += " AND level = ?"
        params.append(level_filter)

    if pool_filter:
        query += " AND pool = ?"
        params.append(pool_filter)
    if source_filter:
        query += " AND source = ?"
        params.append(source_filter)
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    direction = 'ASC' if direction.upper() == 'ASC' else 'DESC'
    query += f" ORDER BY {sort_by} {direction}"
    query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]

    count_query = "SELECT COUNT(*) as total FROM logs WHERE 1=1"
    if search or type_filter or collector_filter or level_filter or pool_filter or source_filter or start_date or end_date:
        count_query += query[query.index(" AND"):query.index(" ORDER BY")]

    cursor2 = conn.cursor()
    cursor2.execute(count_query, params[:-2])
    total = cursor2.fetchone()[0]
    conn.close()
    return rows, total

def sqlite_is_empty():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM logs")
    count = c.fetchone()[0]
    conn.close()
    return count == 0

def should_ingest_file(file_path: str, size: int, mtime: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT size, mtime FROM file_ingest_state WHERE file_path = ?",
        (file_path,)
    )
    row = cursor.fetchone()
    conn.close()

    if row is None:
        return True  # never seen before

    return row["size"] != size or row["mtime"] != mtime

from datetime import datetime as _dt

def mark_file_ingested(file_path: str, size: int, mtime: int):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        '''
        INSERT OR REPLACE INTO file_ingest_state
        (file_path, size, mtime, last_ingested_at)
        VALUES (?, ?, ?, ?)
        ''',
        (file_path, size, mtime, _dt.utcnow().isoformat())
    )

    conn.commit()
    conn.close()

