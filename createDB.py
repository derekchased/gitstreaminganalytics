import sqlite3
from sqlite3 import Error

db = "gitstream.db"
conn = None
try:
    conn = sqlite3.connect(db)
except Error as e:
    print(e)

sql = """
    CREATE TABLE IF NOT EXISTS projects (
    name text PRIMARY KEY,
    language text,
    commits integer DEFAULT 0,
    cicd integer DEFAULT 0,
    test integer DEFAULT 0
    ); 
    """
try:
    c = conn.cursor()
    c.execute(sql)
except Error as e:
    print(e)
