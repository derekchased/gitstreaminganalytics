import pulsar
import pymongo
import json
import time


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q1 = client.subscribe('topic_q1_2_1', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)


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
    

start = time.time()
while True:
    msg_q1 = consumer_q1.receive()
    
    try:
        data_q1 = msg_q1.data()
        store_q1(data_q1)
        
        # end = time.time()
        # print('curr time: ', end-start)
        
        consumer_q1.acknowledge(msg_q1)
    except:
        consumer_q1.negative_acknowledge(msg_q1)       
   
