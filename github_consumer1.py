import pulsar
#import pymongo


RESULTS = {}


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')
# Subscribe to a topic and subscription
consumer = client.subscribe('languages_topic', subscription_name='github_sub_1')

# mongodb client
# mongo_db_client = pymongo.MongoClient("mongodb://localhost:27017/")
# results_db = mongo_db_client["results_database"]
# language_collection = results_db["languages"]


def store_results(data):
    if data not in RESULTS.keys():
        RESULTS[data] = 1
    else:
        RESULTS[data] += 1


while True:
    msg = consumer.receive()
    
    try:
        data = msg.data()
        
        store_results(data)
        
        print('current RESULTS: ')
        for key, val in RESULTS.items():
            print(key, val)

        consumer.acknowledge(msg)

    except:
        consumer.negative_acknowledge(msg)