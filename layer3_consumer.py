import pulsar
import pymongo
import json

RESULTS = {}


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q1 = client.subscribe('topic_q1_layer23', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)
consumer_q2 = client.subscribe('topic_q2_layer23', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)
consumer_q3 = client.subscribe('topic_q3_layer23', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)
consumer_q4 = client.subscribe('topic_q4_layer23', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)


# mongodb client
# mongo_db_client = pymongo.MongoClient("mongodb://localhost:27017/")
# results_db = mongo_db_client["results_database"]
# language_collection = results_db["languages"]


def store_q1(data):
    if data not in RESULTS.keys():
        RESULTS[data] = 1
    else:
        RESULTS[data] += 1
        
        
def store_q2(project_name, num_commits):
    if project_name not in RESULTS.keys():
        RESULTS[project_name] = num_commits
    else:
        RESULTS[project_name] += num_commits
        
def store_q3(data):
    if data not in RESULTS.keys():
        RESULTS[data] = 1
    else:
        RESULTS[data] += 1
        
def store_q4(data):
    if data not in RESULTS.keys():
        RESULTS[data] = 1
    else:
        RESULTS[data] += 1


while True:
    msg_q1 = consumer_q1.receive()
    # msg_q2 = consumer_q2.receive()
    # msg_q3 = consumer_q3.receive()
    # msg_q4 = consumer_q4.receive()
    
    if msg_q1:
        try:
            data_q1 = msg_q1.data()
            store_q1(data_q1)
            consumer_q1.acknowledge(msg_q1)
        except:
            consumer_q1.negative_acknowledge(msg_q1)       
    # if msg_q2:
    #     try:
    #         data_q2 = msg_q2.data()
            
    #         data_json = json.loads(data_q2)
    #         project_name = list(data_json.keys())[0]
    #         num_commits = data_json[project_name]  
    #         store_q2(project_name, num_commits)
            
    #         consumer_q2.acknowledge(msg_q2)
    #     except:
    #         consumer_q2.negative_acknowledge(msg_q2)
            
    # if msg_q3:
    #     try:
    #         data_q3 = msg_q3.data()
    #         store_q3(data_q3)
    #         consumer_q3.acknowledge(msg_q3)
    #     except:
    #         consumer_q3.negative_acknowledge(msg_q3)
            
    # if msg_q4:
    #     try:
    #         data_q4 = msg_q4.data()
    #         store_q4(data_q4)
    #         consumer_q4.acknowledge(msg_q4)
    #     except:
    #         consumer_q4.negative_acknowledge(msg_q4)