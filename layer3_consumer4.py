import pulsar
import pymongo
import json
import time


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q4 = client.subscribe('topic_q4_2_1', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)


# mongodb client
# mongo_db_client = pymongo.MongoClient("mongodb://localhost:27017/")
# results_db = mongo_db_client["results_database"]
# language_collection = results_db["languages"]
        
RESULTS_Q4 = {}
def store_q4(data):
    if data not in RESULTS_Q4.keys():
        RESULTS_Q4[data] = 1
    else:
        RESULTS_Q4[data] += 1


start = time.time()
while True:
    msg_q4 = consumer_q4.receive()
    try:
        data_q4 = msg_q4.data()
        store_q4(data_q4)
        
        consumer_q4.acknowledge(msg_q4)
    except:
        consumer_q4.negative_acknowledge(msg_q4)