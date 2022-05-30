import pulsar
import json
import time


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q134 = client.subscribe('topic_q134_2', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)


RESULTS_Q134 = {}   
def store_q2(project_name, language, has_tests, has_cont_int):
    RESULTS_Q134['project_name'] = project_name
    RESULTS_Q134['language'] = language
    RESULTS_Q134['has_tests'] = has_tests
    RESULTS_Q134['has_cont_int'] = has_cont_int
    

count=0
start = time.time()
while True:
    msg_q2 = consumer_q134.receive()

   
    try:
        data_q134 = msg_q2.data()
        
        data_json = json.loads(data_q134)
        
        project_name = data_json['project_name']
        language = data_json['language']
        has_tests = data_json['has_tests'] # returns boolean
        has_cont_int = data_json['has_cont_int'] # returns boolean
        
        # TODO: store  in database
        store_q2(project_name, language, has_tests, has_cont_int)
        
        for k, v in RESULTS_Q134.items():
            print(k, v)
            
        consumer_q134.acknowledge(msg_q2)
    except:
        consumer_q134.negative_acknowledge(msg_q2)
    