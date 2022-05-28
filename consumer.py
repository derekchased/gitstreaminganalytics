import pulsar
import pymongo


RESULTS = {}


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q1 = client.subscribe('topic_q1', subscription_name='github_sub_1')
consumer_q2 = client.subscribe('topic_q2', subscription_name='github_sub_1')
consumer_q3 = client.subscribe('topic_q3', subscription_name='github_sub_1')
consumer_q4 = client.subscribe('topic_q4', subscription_name='github_sub_1')


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
    msg_q1 = consumer_q1.receive()
    msg_q2 = consumer_q2.receive()
    msg_q3 = consumer_q3.receive()
    msg_q4 = consumer_q4.receive()
    
    try:
        data_q1 = msg_q1.data()
        data_q2 = msg_q2.data()
        data_q3 = msg_q3.data()
        data_q4 = msg_q4.data()

        store_results(data)
        
        print('current RESULTS: ')
        for key, val in RESULTS.items():
            print(key, val)

        consumer.acknowledge(msg)

    except:
        consumer.negative_acknowledge(msg)