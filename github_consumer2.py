import pulsar
import json
#import pymongo


RESULTS = {}


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')
# Subscribe to a topic and subscription
consumer = client.subscribe('commits_topic', subscription_name='github_sub_1')

# mongodb client
# mongo_db_client = pymongo.MongoClient("mongodb://localhost:27017/")
# results_db = mongo_db_client["results_database"]
# language_collection = results_db["languages"]


def store_results(project_name,num_commits):
    if project_name not in RESULTS.keys():
        RESULTS[project_name] = num_commits
    else:
        RESULTS[project_name] += num_commits


while True:
    msg = consumer.receive()
    try:
        #print("Received message : ", msg.data())
        data = msg.data()
        
        data_json = json.loads(data)
        project_name = list(data_json.keys())[0]
        num_commits = data_json[project_name]        
        # TODO: Store data in MongoDB?!
        store_results(project_name,num_commits)
        # print('current RESULTS: ')
        # for key, val in RESULTS.items():
        #     print(key, val)
        consumer.acknowledge(msg)

    except:
        consumer.negative_acknowledge(msg)