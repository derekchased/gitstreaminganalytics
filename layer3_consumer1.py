import imp
import pulsar
import json
import time
import sqlite3
from sqlite3 import Error

# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q2 = client.subscribe('topic_q2_2', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)
db = "gitstream.db"
#...     CREATE TABLE IF NOT EXISTS projects (
#...     name text PRIMARY KEY,
#...     language text,
#...     commits integer DEFAULT 0,
#...     test integer DEFAULT 0,
#...     cicd integer DEFAULT 0
#...     ); 

conn = None
try:
    conn = sqlite3.connect(db)
except Error as e:
    print(e)


def store_q2(project_name, num_commits):
    #https://www.sqlite.org/draft/lang_UPSERT.html
    sql = """
            INSERT OR REPLACE into projects (
                name,
                language,
                commits,
                test,
                cicd
            ) VALUES (?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET commits=excluded.commits;
            """
    cur = conn.cursor()
    cur.execute(sql,(project_name,"",num_commits,0,0))
    conn.commit()   

count=0
start = time.time()
while True:
    msg_q2 = consumer_q2.receive()

   
    try:
        data_q2 = msg_q2.data()
        
        data_json = json.loads(data_q2)
        project_name = list(data_json.keys())[0]
        num_commits = data_json[project_name]  
        # TODO: Store in Database
        store_q2(project_name, num_commits)
        
        count+=1
        print('count: ', count)
        
        consumer_q2.acknowledge(msg_q2)
    except:
        consumer_q2.negative_acknowledge(msg_q2)
    