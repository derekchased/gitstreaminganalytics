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


def get_num_commits(dictionary, token):
    """
    Returns the number of commits for a given project
    
    Input: dictionary that contains repository information
    """
    commits_url = dictionary["commits_url"] # returns of form 'https://api.github.com/repos/sindrets/diffview.nvim/commits{/sha}'
    # remove suffix so it can be used for api call
    try:
        commits_url = commits_url.removesuffix("{/sha}")
    except Exception as e:
        print(e)
    
    # issue request
    headers = {'Authorization': f'token {token}'}
    try:
        r = requests.get(commits_url, headers=headers)
        r = r.json()
    except Exception as e:
        print(e) 
    return len(r) # length of this list correspons to the number of commits
    
            
def get_programming_language(dictionary):
    """
    Returns the programming language for a given project
    
    Input: dictionary that contains repository information
    """
    language = dictionary["language"]
    return language


def query_github_languages(start_date: datetime, num_days: int, tokens: list):
    """Makes calls to github API and sends received data to consumer"""
    curr_date = start_date

    for _ in range(num_days):
        for token in tokens: # enables possibility to exceed api limit through using different tokens
            for j in range(10):
                # set token for query request
                headers = {'Authorization': f'token {token}'}
                query_url = f"https://api.github.com/search/repositories?q=created:{curr_date}..{curr_date}&per_page=100&page={j}"
                # issue API request
                try:
                    req = requests.get(query_url, headers=headers)
                except Exception as e:
                    print(e) 
                # transform to json
                req_json  = req.json()
                # only get necessary information
                try:
                    ls_of_dicts = req_json["items"] # returns list

                    # iterate through list and send 'language' value to consumer
                    for dictionary in ls_of_dicts:
                        # get number of commits of porject
                        num_commits = get_num_commits(dictionary, token) #TODO: send it to producer
                        # send value of key "language" to producer 
                        lang_res = dictionary["language"]
                        # make sure it's indeed a string
                        if isinstance(lang_res, str):
                            # send to pulsar consumer
                            producer.send((lang_res).encode('utf_8'))
                        else:
                            pass
                except KeyError as e:
                    print(e)                    
                    print('KeyError when selecting "items"')

        # increment day
        curr_date += datetime.timedelta(days=1)
        print(curr_date)
        # sleep some time to reset limit
        # TODO: how long should sleep be?
        # time.sleep(90)
        



if __name__=="__main__":
    # get github tokens
    tokens = get_tokens(["githubtoken_jonas.txt"])
    
    start = time.time()
    # query github for next 3 days
    query_github_languages(datetime.date(2021, 5, 1), 30, tokens)
    end = time.time()
    print('duration: ', end-start)