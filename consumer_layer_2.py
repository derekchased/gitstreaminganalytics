import pulsar
import json
import requests
import time

# Create a pulsar client by supplying ip address and port
client = pulsar.Client('pulsar://localhost:6650')
# Subscribe to a topic and subscription
consumer = client.subscribe('question134', subscription_name='github_sub_1')

# create producer 
#producer_layer_2 = client.create_producer('')

RESULTS_q1 = {}
def store_programming_languages(data):
    if data not in RESULTS_q1.keys():
        RESULTS_q1[data] = 1
    else:
        RESULTS_q1[data] += 1
        
RESULTS_q3 = {}
def store_tests(data):
    if data not in RESULTS_q3.keys():
        RESULTS_q3[data] = 1
    else:
        RESULTS_q3[data] += 1
        
RESULTS_q4 = {}
def store_ci(data):
    if data not in RESULTS_q4.keys():
        RESULTS_q4[data] = 1
    else:
        RESULTS_q4[data] += 1
        
        
        
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
                continue
            return req
                   

def get_programming_language(dictionary):
    """
    Returns the programming language for a given project
    
    Input: dictionary that contains repository information
    """
    language = dictionary["language"]
    
    # send to pulsar consumer
    if isinstance(language, str):
        # producer_layer_2.send((language).encode('utf_8'))
        store_programming_languages(language)
        return language
    else:
        pass
    
def get_unit_tests(dictionary, language,tokens):
    """ 
    Returns boolean whether there is a unit test in directory.
    Returns the query url for function 'get_continuous_integration'
    
    Input: dictionary, name of programming language, tokens
    """
    query_url3 = dictionary["contents_url"][0:-7] 
    req = call_api(query_url3,tokens)
    for item in req.json():
        if('test' in item['name']):
            # send language that contains unit tests to producer
            #producer_layer_2.send((language).encode('utf_8'))
            store_tests(language)
            return True, query_url3
    return False, query_url3

def get_continuous_integration(query_url3, language,tokens):
    # https://docs.github.com/en/actions/learn-github-actions/understanding-github-actions    
    query_url4 = query_url3+".github/workflows"
    req = call_api(query_url4,tokens)
    if(req != False):
    # if it hasnt have workflow directory, then it will return message not found, otherwise it
    # will return the object with all the items in such directory (and the indexs will be integer)
    # message: "not found"
    # in this case the workflow directory doesn't exist
    # if it exists, there is no message, i.e., TypeError, send it to producer
        
        #producer_4.send((language).encode('utf_8'))
        store_ci(language)

## CONSUMER AND PRODUCER ##
tokens = get_tokens(["githubtoken_jonas.txt", "githubtoken_alvaro.txt"])
start = time.time()

while True:
    msg = consumer.receive()
    try:
        data = msg.data()
        dictionary = json.loads(data)
        
        language = get_programming_language(dictionary)        
        
        # Q3 Tests
        if (language is not None):                
            has_test, query_url3 = get_unit_tests(dictionary,language,tokens)
        #Q4 CI/CD
        if(has_test):                          
            get_continuous_integration(query_url3,language,tokens)
        consumer.acknowledge(msg)
        
        end = time.time()
        print('curr time: ', end-start)
        
        # # prints
        # print('current RESULTS_q1: ')
        # for key, val in RESULTS_q1.items():
        #     print(key, val) 
        
        # print("\n")
        
        # print('current RESULTS_q3: ')
        # for key, val in RESULTS_q3.items():
        #     print(key, val) 
        
        # print("\n")
  
        # print('current RESULTS_q4: ')
        # for key, val in RESULTS_q4.items():
        #     print(key, val) 

    except:
        consumer.negative_acknowledge(msg)