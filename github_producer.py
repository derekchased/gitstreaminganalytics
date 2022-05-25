from curses import reset_shell_mode
from email import header
import requests
import os
import datetime
import pulsar


# create producer
client = pulsar.Client('pulsar://localhost:6650')

# Create a producer on the topic that consumer can subscribe to
producer = client.create_producer('languages_topic')

# get github token
with open('githubtoken.txt', 'r') as file:
    GITHUB_TOKEN = file.read().rstrip()


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


def query_github(start_date: datetime, num_days: int, key="language", token=GITHUB_TOKEN) -> list:
    """Makes calls to github API and sends received data to consumer"""
    curr_date = start_date

    for _ in range(num_days):
        for j in range(3):
            print(curr_date)
            query_url = f"https://api.github.com/search/repositories?q=created:{curr_date}..{curr_date}&per_page=100&page={j}"
            # issue API request
            req = requests.get(query_url)
            # transform to json
            req_json  = req.json()
            # only get necessary information
            req_json = req_json["items"]

            res_ls = get_values_by_key(req_json, key)

            # producer should send data to consumer. 
            for res in res_ls:
                if isinstance(res, str):
                    producer.send((res).encode('utf_8'))
                else:
                    pass

        # increment day
        curr_date += datetime.timedelta(days=1)


# query github for next 2 days
query_github(datetime.date(2021, 5, 1), 2)