# utils/db_utils.py
import sqlite3

def connect_to_db(db_name):
    conn = sqlite3.connect(db_name, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def create_snapshot(conn):
    cursor = conn.cursor()
    cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
    snapshot_id = cursor.lastrowid
    return snapshot_id