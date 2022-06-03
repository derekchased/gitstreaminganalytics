import pulsar
import json
import requests

# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')
# Subscribe to a topic and subscription
consumer = client.subscribe('topic_q2_1', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)

# create producer 
producer_q2_layer2 = client.create_producer('topic_q2_2')

        
def get_tokens(filepaths: list):
    """
    takes list of strings of filepaths to .txt files that contain github token

    e.g., ["filepath_1.txt", "filepath_2.txt"]
    
    returns the stored tokens. 
    """
    tokens = []
    for filepath in filepaths:
        with open(filepath, 'r') as file:
            token = file.read().rstrip()
            tokens.append(token)
    return tokens


def call_api(query_url, tokens):
    """changes token if API limit is exceeded"""
    while True:
        for token in tokens:
            headers = {'Authorization': f'token {token}'}
            try:
                req = requests.get(query_url, headers=headers)
            except Exception as e:
                print(e)
            status = req.status_code
            if (status == 409):
                break
            if (status == 404):
                return False
            if(status != 200):
                print('changing token')
                print(req.status_code)
                continue
            return req
        break
                   
                   
def get_num_commits(dictionary, tokens, project_name):
    """
    Returns the number of commits for a given project
    
    Input: dictionary that contains repository information
    """
    commits_url = dictionary["commits_url"] # returns of form 'https://api.github.com/repos/sindrets/diffview.nvim/commits{/sha}'
    # remove suffix so it can be used for api call
    commits_url = commits_url[:-6] + f"?=&per_page=1&page=1" 
    # issue request
    res = call_api(commits_url, tokens)
    if res.links:
        num_commits = res.links['last']['url'].split('&page=')[1] 
    output = json.dumps({project_name: num_commits})
    producer_q2_layer2.send((output).encode('utf_8'))
        

tokens = get_tokens(["githubtoken_jonas.txt", "githubtoken_alvaro.txt"])
## CONSUMER AND PRODUCER ##
while True:
    msg = consumer.receive()
    try:
        data = msg.data()
        dictionary = json.loads(data)
        
        get_num_commits(dictionary, tokens, dictionary["full_name"])
        
        consumer.acknowledge(msg)
    
    except:
        consumer.negative_acknowledge(msg)
        
