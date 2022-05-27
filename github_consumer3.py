import pulsar
#import pymongo


RESULTS = {}


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')
# Subscribe to a topic and subscription
consumer = client.subscribe('tests_topic', subscription_name='github_sub_1')

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
        #print("Received message : ", msg.data())
        data = msg.data()
        
        # TODO: Store data in MongoDB?!
        store_results(data)
        # print('current RESULTS: ')
        # for key, val in RESULTS.items():
        #     print(key, val)
        consumer.acknowledge(msg)

    except:
        consumer.negative_acknowledge(msg)