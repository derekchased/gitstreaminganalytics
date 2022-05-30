import pulsar
import json
import time


# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_q2 = client.subscribe('topic_q134_2', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)


RESULTS_Q2 = {}   
def store_q2(project_name, num_commits):
    if project_name not in RESULTS_Q2.keys():
        RESULTS_Q2[project_name] = num_commits
    else:
        RESULTS_Q2[project_name] += num_commits
   

count=0
start = time.time()
while True:
    msg_q2 = consumer_q2.receive()

   
    try:
        data_q2 = msg_q2.data()
        
        data_json = json.loads(data_q2)
        project_name = list(data_json.keys())[0]
        num_commits = data_json[project_name]  
        store_q2(project_name, num_commits)
        
        count+=1
        print('count: ', count)
        
        consumer_q2.acknowledge(msg_q2)
    except:
        consumer_q2.negative_acknowledge(msg_q2)
    