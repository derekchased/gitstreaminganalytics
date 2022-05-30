import pulsar
import json
import time


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q134 = client.subscribe('topic_q134_2', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)


RESULTS_Q2 = {}   
def store_q2(project_name, num_commits):
    if project_name not in RESULTS_Q2.keys():
        RESULTS_Q2[project_name] = num_commits
    else:
        RESULTS_Q2[project_name] += num_commits
   

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
        # store_q2(project_name, num_commits)
        
        count+=1
        print('count: ', count)
        
        consumer_q134.acknowledge(msg_q2)
    except:
        consumer_q134.negative_acknowledge(msg_q2)
    