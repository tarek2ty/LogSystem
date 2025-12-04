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
