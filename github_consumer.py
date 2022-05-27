import pulsar
#import pymongo


RESULTS = {}


# producer_1 = client.create_producer('languages_topic')
# producer_2 = client.create_producer('commits_topic')
# producer_3 = client.create_producer('tests_topic')
# producer_4 = client.create_producer('cont_int_topic')

# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')
# Subscribe to a topic and subscription
consumer_1 = client.subscribe('languages_topic', subscription_name='github_sub_1')
consumer_2 = client.subscribe('commits_topic', subscription_name='github_sub_1')
consumer_3 = client.subscribe('tests_topic', subscription_name='github_sub_1')
consumer_4 = client.subscribe('cont_int_topic', subscription_name='github_sub_1')

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
    msg1 = consumer_1.receive()
    msg2 = consumer_2.receive()
    msg3 = consumer_3.receive()
    msg4 = consumer_4.receive()
    try:
        #print("Received message : ", msg.data())
        data = msg1.data()
        
        # TODO: Store data in MongoDB?!
        store_results(data)
        # print('current RESULTS: ')
        # for key, val in RESULTS.items():
        #     print(key, val)


        consumer_1.acknowledge(msg1)
        consumer_2.acknowledge(msg2)
        consumer_3.acknowledge(msg3)
        consumer_4.acknowledge(msg4)


    except:
        consumer_1.negative_acknowledge(msg1)
        consumer_2.negative_acknowledge(msg2)
        consumer_3.negative_acknowledge(msg3)
        consumer_4.negative_acknowledge(msg4)