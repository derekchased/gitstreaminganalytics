import pulsar
import json
import requests
import time



# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')

# Subscribe to a topic and subscription
consumer_layer_1 = client.subscribe('question2', subscription_name='github_sub_1')

# create producer 
#producer_layer_1 = client.create_producer('tests_contint_topic')


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
    while True:
        for token in tokens:
            headers = {'Authorization': f'token {token}'}
            try:
                req = requests.get(query_url, headers=headers)
            except Exception as e:
                print(e)
            status = req.status_code
            if (status == 404):
                return False
            if(status != 200):
                print('changing token')
                print('req.status_code')
                continue
            return req
                   
                   
def get_num_commits(dictionary, tokens, project_name):
    """
    Returns the number of commits for a given project
    
    Input: dictionary that contains repository information
    """
    commits_url = dictionary["commits_url"] # returns of form 'https://api.github.com/repos/sindrets/diffview.nvim/commits{/sha}'
    # remove suffix so it can be used for api call
    commits_url = commits_url[:-6]
    
    # issue request
    r = call_api(commits_url,tokens)
    r = r.json()
    num_commits = len(r) # length of this list correspons to the number of commits

    #output = json.dumps({project_name: num_commits})
    
    #producer_layer_1.send((output).encode('utf_8'))
    
    # store data here
    #data_json = json.loads(output)
    # project_name = list(data_json.keys())[0]
    # num_commits = data_json[project_name]        

    store_results(project_name,num_commits)


RESULTS={}
def store_results(project_name,num_commits):
    if project_name not in RESULTS.keys():
        RESULTS[project_name] = num_commits
    else:
        RESULTS[project_name] += num_commits
        
        

## CONSUMER AND PRODUCER ##
tokens = get_tokens(["githubtoken_jonas.txt", "githubtoken_alvaro.txt"])

start = time.time()
while True:
    msg = consumer_layer_1.receive()
    try:
        data = msg.data()
        dictionary = json.loads(data)
        
        # SENDS it to next layer (consumer_layer_2)
        get_num_commits(dictionary, tokens, project_name=dictionary["name"])
        # print('current RESULTS: ')
        # for key, val in RESULTS.items():
        #     print(key, val) 
        consumer_layer_1.acknowledge(msg)
        end = time.time()
        print('curr time: ', end-start)

    except:
        consumer_layer_1.negative_acknowledge(msg)
        
