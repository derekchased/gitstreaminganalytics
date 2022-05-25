from curses import reset_shell_mode
from email import header
import requests
import datetime
import pulsar
import time


# create producer
client = pulsar.Client('pulsar://localhost:6650')

# Create a producer on the topic that consumer can subscribe to
producer = client.create_producer('languages_topic')


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


def get_values_by_key(github_json: dict, key: str) -> list:
    """takes result of github query and returns the key (e.g., programming language)"""
    # 
    ls = list(github_json.values())
    # get third element, contains meta-information
    vals = ls[2]
    # iterate through list and return value of given key
    stored_values = []
    for val in vals:
        stored_values.append(val[key])
    return stored_values


def query_github_languages(start_date: datetime, num_days: int, tokens: list) -> list:
    """Makes calls to github API and sends received data to consumer"""
    curr_date = start_date

    for _ in range(num_days):
        for j in range(10):
            for token in tokens:
                # set token for query request
                headers = {'Authorization': f'token {token}'}
                query_url = f"https://api.github.com/search/repositories?q=created:{curr_date}..{curr_date}&per_page=100&page={j}"
                # issue API request
                req = requests.get(query_url, headers=headers)
                # transform to json
                req_json  = req.json()
                # only get necessary information
                ls_of_dicts = req_json["items"] # returns list

                # iterate through list and send 'language' value to consumer
                for dictionary in ls_of_dicts:
                    # select key "language"
                    lang_res = dictionary["language"]
                    # make sure it's indeed a string
                    if isinstance(lang_res, str):
                        # send to pulsar consumer
                        producer.send((lang_res).encode('utf_8'))
                    else:
                        pass
        # increment day
        curr_date += datetime.timedelta(days=1)



if __name__=="__main__":
    # get github tokens
    tokens = get_tokens(["githubtoken_jonas.txt"])
    # query github for next 2 days
    query_github_languages(datetime.date(2021, 5, 1), 2, tokens)