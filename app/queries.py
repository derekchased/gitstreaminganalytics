# %%
import sqlite3
from sqlite3 import Error

# %%
db = "gitstream.db"

# Set number of results for each question
LIMIT = 10

# %%
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

# %%
def query(conn,sql,limit):
    cur = conn.cursor()
    cur.execute(sql,(limit,))
    rows = cur.fetchall()

    for row in rows:
        print(row)

# %%
conn = create_connection(db)


#Q1. What are the top 10 programming languages based on the number of projects developed?
sql1 = """ 
        SELECT language, count(*) FROM projects 
        GROUP BY language ORDER BY count(*) DESC 
        LIMIT ?;  
        """
#Q2. What are the top 10 most frequently updated GitHub projects (i.e., most commits in a project)?
sql2 = """
        SELECT name,commits FROM projects 
        ORDER BY commits DESC 
        LIMIT ?;
        """
#Q3. What are the top 10 programming languages that follow the test-driven development
#approach (i.e., most projects with unit tests)?
sql3 = """ 
        SELECT language, count(*) FROM projects 
        WHERE test>0 GROUP BY language 
        ORDER BY count(*) DESC LIMIT ?;
        """
#Q4. What are the top 10 programming languages that follow test-driven development and DevOps
#approach (i.e., uses continuous integration in the development)?
sql4 = """
        SELECT language, count(*) FROM projects 
        WHERE test>0 AND cicd>0 GROUP BY language 
        ORDER BY count(*) DESC LIMIT ?;
        """

with conn:
        print("===========")
        print("Results Q1:")
        query(conn,sql1,LIMIT)
        print("===========")
        print("Results Q2:")
        query(conn,sql2,LIMIT)
        print("===========")
        print("Results Q3:")
        query(conn,sql3,LIMIT)
        print("===========")
        print("Results Q4:")
        query(conn,sql4,LIMIT)
        print("===========")


