import pulsar
import pymongo
import json
import time


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription

consumer_q3 = client.subscribe('topic_q3_2_1', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)


# mongodb client
# mongo_db_client = pymongo.MongoClient("mongodb://localhost:27017/")
# results_db = mongo_db_client["results_database"]
# language_collection = results_db["languages"]

        
RESULTS_Q3 = {}
def store_q3(data):
    if data not in RESULTS_Q3.keys():
        RESULTS_Q3[data] = 1
    else:
        RESULTS_Q3[data] += 1
        


start = time.time()
while True:
    msg_q3 = consumer_q3.receive()

    try:
        data_q3 = msg_q3.data()
        store_q3(data_q3)

        consumer_q3.acknowledge(msg_q3)
    except:
        consumer_q3.negative_acknowledge(msg_q3)