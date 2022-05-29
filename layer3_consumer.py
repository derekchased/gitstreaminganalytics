import pulsar
import pymongo
import json
import time

RESULTS = {}


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q1 = client.subscribe('topic_q1_2', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)
consumer_q2 = client.subscribe('topic_q2_2', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)
consumer_q3 = client.subscribe('topic_q3_2', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)
consumer_q4 = client.subscribe('topic_q4_2', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)


# mongodb client
# mongo_db_client = pymongo.MongoClient("mongodb://localhost:27017/")
# results_db = mongo_db_client["results_database"]
# language_collection = results_db["languages"]

RESULTS_Q1 = {}
def store_q1(data):
    if data not in RESULTS_Q1.keys():
        RESULTS_Q1[data] = 1
    else:
        RESULTS_Q1[data] += 1
        
RESULTS_Q2 = {}   
def store_q2(project_name, num_commits):
    if project_name not in RESULTS_Q2.keys():
        RESULTS_Q2[project_name] = num_commits
    else:
        RESULTS_Q2[project_name] += num_commits
        
RESULTS_Q3 = {}
def store_q3(data):
    if data not in RESULTS_Q3.keys():
        RESULTS_Q3[data] = 1
    else:
        RESULTS_Q3[data] += 1
        
RESULTS_Q4 = {}
def store_q4(data):
    if data not in RESULTS_Q4.keys():
        RESULTS_Q4[data] = 1
    else:
        RESULTS_Q4[data] += 1

start = time.time()

while True:
    msg_q1 = consumer_q1.receive()
    msg_q2 = consumer_q2.receive()
    msg_q3 = consumer_q3.receive()
    msg_q4 = consumer_q4.receive()
    
    if msg_q1:
        print('received msg Q1')
        try:
            data_q1 = msg_q1.data()
            store_q1(data_q1)
            
            end = time.time()
            print('curr time: ', end-start)
            
            consumer_q1.acknowledge(msg_q1)
        except:
            consumer_q1.negative_acknowledge(msg_q1)       
    if msg_q2:
        print('received msg Q1')
        try:
            data_q2 = msg_q2.data()
            
            data_json = json.loads(data_q2)
            project_name = list(data_json.keys())[0]
            num_commits = data_json[project_name]  
            store_q2(project_name, num_commits)
            
            consumer_q2.acknowledge(msg_q2)
        except:
            consumer_q2.negative_acknowledge(msg_q2)
            
    if msg_q3:
        print('received msg Q3')
        try:
            data_q3 = msg_q3.data()
            store_q3(data_q3)
    
            consumer_q3.acknowledge(msg_q3)
        except:
            consumer_q3.negative_acknowledge(msg_q3)
            
    if msg_q4:
        print('received msg Q4')
        try:
            data_q4 = msg_q4.data()
            store_q4(data_q4)
            consumer_q4.acknowledge(msg_q4)
        except:
            consumer_q4.negative_acknowledge(msg_q4)