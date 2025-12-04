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
