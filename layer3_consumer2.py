import pulsar
import json
import time
import sqlite3
from sqlite3 import Error

# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q134 = client.subscribe('topic_q134_2', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)
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


def store(project_name, language, has_tests, has_cont_int):
    #https://www.sqlite.org/draft/lang_UPSERT.html
    sql = """
            INSERT OR REPLACE into projects (
                name,
                language,
                commits,
                test,
                cicd
            ) VALUES (?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET 
                language=excluded.language,
                test=excluded.test,
                cicd=excluded.cicd;
            """
    cur = conn.cursor()
    if has_tests:
        test = 1
    else:
        test = 0
    if has_cont_int:
        cicd = 1
    else:  
        cicd = 0

    cur.execute(sql,(project_name,language,0,test,cicd))
    conn.commit()   
   
count=0
start = time.time()
while True:
    msg_q2 = consumer_q134.receive()

   
    try:
        data_q134 = msg_q2.data()
        
        data_json = json.loads(data_q134)
        
        project_name = data_json['project_name']
        language = data_json['language']
        has_tests = data_json['has_tests'] # returns boolean
        has_cont_int = data_json['has_cont_int'] # returns boolean
        
        # TODO: store  in database
        store(project_name, language, has_tests, has_cont_int)
                    
        consumer_q134.acknowledge(msg_q2)
    except:
        consumer_q134.negative_acknowledge(msg_q2)
    