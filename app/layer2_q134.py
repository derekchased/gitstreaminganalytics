import pulsar
import json
import requests

# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')
# Subscribe to a topic and subscription
consumer = client.subscribe('topic_q134_1', subscription_name='github_sub_1', consumer_type=pulsar.ConsumerType.Shared)

# create producer 
producer_layer2 = client.create_producer('topic_q134_2')


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
            if (status == 404):
                return False
            if(status != 200):
                print('changing token')
                continue
            return req
 
 
def get_project_name(dictionary):
    """
    Returns the programming language for a given project
    
    Input: dictionary that contains repository information
    """
    project_name = dictionary["full_name"]
    
    if isinstance(project_name, str):
        return project_name
    else:
        pass
                      

def get_programming_language(dictionary):
    """
    Returns the programming language for a given project
    
    Input: dictionary that contains repository information
    """
    language = dictionary["language"]
    project_name = get_project_name(dictionary)
    
    # send to pulsar consumer
    if isinstance(language, str):
        return language
    else:
        pass
    
    
def get_unit_tests(dictionary,tokens):
    """ 
    Returns boolean whether there is a unit test in directory.
    Returns the query url for function 'get_continuous_integration'
    
    Input: dictionary, name of programming language, tokens
    """
    has_tests = False
    project_name = get_project_name(dictionary)
    query_url3 = dictionary["contents_url"][0:-7] 
    req = call_api(query_url3,tokens)
    for item in req.json():
        if('test' in item['name']):
            has_tests = True
            return has_tests, query_url3
    return has_tests, query_url3


def get_continuous_integration(query_url3 ,tokens):
    # https://docs.github.com/en/actions/learn-github-actions/understanding-github-actions    
    query_url4 = query_url3+".github/workflows"
    req = call_api(query_url4,tokens)
    # if it hasnt have workflow directory, then it will return message not found, otherwise it
    # will return the object with all the items in such directory (and the indexs will be integer)
    # message: "not found"
    # in this case the workflow directory doesn't exist
    # if it exists, there is no message, i.e., TypeError, send it to producer
    if(req != False):
        return True
    else:
        return False # if it has no cont int

def send_to_producer(dictionary, tokens):
    project_name = get_project_name(dictionary)
    # Q1
    language = get_programming_language(dictionary)
    # Q3
    has_tests, query_url3 = get_unit_tests(dictionary, tokens)
    print(has_tests)
    # Q4
    has_cont_int = False
    if has_tests:
        has_cont_int = get_continuous_integration(query_url3, tokens)

    output = json.dumps({'project_name': project_name, 
                         'language': language,
                         'has_tests': has_tests, 
                         'has_cont_int': has_cont_int})
    # send to producer
    producer_layer2.send((output).encode('utf_8'))
    

## CONSUMER AND PRODUCER ##
tokens = get_tokens(["githubtoken_jonas.txt", "githubtoken_alvaro.txt"])
while True:
    msg = consumer.receive()
    try:
        data = msg.data()
        dictionary = json.loads(data)
        
        send_to_producer(dictionary, tokens)
        consumer.acknowledge(msg)

    except:
        consumer.negative_acknowledge(msg)