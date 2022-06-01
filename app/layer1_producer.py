#from email import header
#from subprocess import call
import requests
import datetime
import pulsar
import time
import json

# create producer
client = pulsar.Client('pulsar://localhost:6650')

# Create a producer on the topic that consumer can subscribe to
producer_q2 = client.create_producer('topic_q2_111')
producer_q134 = client.create_producer('topic_q134_111')


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


# def call_api(query_url, tokens):
#     while True:
#         for token in tokens:
#             headers = {'Authorization': f'token {token}'}
#             try:
#                 req = requests.get(query_url, headers=headers)
#             except Exception as e:
#                 print(e)
#             status = req.status_code
#             if (status == 404):
#                 return False
#             if(status != 200):
#                 print('changing token')
#                 continue
#             return req
        
TOKEN_INDEX = 0

def call_api(query_url, tokens):
    global TOKEN_INDEX    
    curr_token = tokens[TOKEN_INDEX % len(tokens)]
       
    headers = {'Authorization': f'token {curr_token}'}
    try:
        req = requests.get(query_url, headers=headers)
        return req
    except Exception as e:
            print(e)
    status = req.status_code
    if (status == 404):
        return False
    if(status != 200):
        print('changing token index')
        TOKEN_INDEX += 1
    return req
                   

def query_github(start_date: datetime, num_days: int, tokens: list):
    """Makes calls to github API and sends received data to consumer"""
    curr_date = start_date

    for day in range(num_days):
        for j in range(5):
            print('j == ', j)
            # set token for query request
            #headers = {'Authorization': f'token {token}'}
            query_url = f"https://api.github.com/search/repositories?q=created:{curr_date}..{curr_date}&per_page=100&page={j}"
            req = call_api(query_url, tokens)
            req_json  = req.json()
            # only get necessary information
            try:
                ls_of_dicts = req_json["items"] # returns list

                # iterate through list and send 'language' value to consumer
                for dictionary in ls_of_dicts:
                    
                    # output = json.dumps({project_name: num_commits})
                    # producer_2.send((output).encode('utf_8'))
                    producer_q134.send(json.dumps(dictionary).encode('utf_8'))
                    producer_q2.send(json.dumps(dictionary).encode('utf_8'))

            except KeyError as e:
                print(e)                    
                print('KeyError when selecting "items"')

        # increment day
        curr_date += datetime.timedelta(days=1)
        print(curr_date)
        

if __name__=="__main__":
    # get github tokens
    tokens = get_tokens(["githubtoken_jonas.txt", "githubtoken_alvaro.txt", "githubtoken_jonas_2.txt", "githubtoken_derek.txt"])
    
    start = time.time()
    # query github for next x days
    query_github(datetime.date(2021, 4, 1), 1, tokens)
    end = time.time()
    print('duration: ', end-start)