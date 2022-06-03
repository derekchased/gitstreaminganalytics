import requests
import datetime
import pulsar
import time
import json

# create producer
client = pulsar.Client('pulsar://localhost:6650')

# Create a producer on the topic that consumer can subscribe to
producer_q2 = client.create_producer('topic_q2_1')
producer_q134 = client.create_producer('topic_q134_1')


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
                   

def query_github(start_date: datetime, num_days: int, tokens: list):
    """Makes calls to github API and sends received data to consumer"""
    curr_date = start_date

    for day in range(num_days):
        for j in range(10):
            print('day == ', day)
            # API request to get main repository information
            query_url = f"https://api.github.com/search/repositories?q=created:{curr_date}..{curr_date}&per_page=100&page={j}"
            req = call_api(query_url, tokens)
            req_json  = req.json()
            # only get necessary information
            try:
                ls_of_dicts = req_json["items"] # returns list

                # iterate through list and send 'language' value to consumer
                for dictionary in ls_of_dicts:
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
    tokens = get_tokens(["githubtoken_jonas.txt", "githubtoken_alvaro.txt"])
    
    start = time.time()
    # query github for next x days
    query_github(datetime.date(2021, 5, 1), 365, tokens)
    end = time.time()
    print('duration: ', end-start)