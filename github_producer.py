from email import header
import requests
import datetime
import pulsar
import time
import json

# create producer
client = pulsar.Client('pulsar://localhost:6650')

# Create a producer on the topic that consumer can subscribe to
producer_1 = client.create_producer('languages_topic')
producer_2 = client.create_producer('commits_topic')
producer_3 = client.create_producer('tests_topic')
producer_4 = client.create_producer('cont_int_topic')


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
    
            
def get_programming_language(dictionary):
    """
    Returns the programming language for a given project
    
    Input: dictionary that contains repository information
    """
    language = dictionary["language"]
    
    # send to pulsar consumer
    if isinstance(language, str):
        producer_1.send((language).encode('utf_8'))
        return language
    else:
        pass
                    
                    
def get_num_commits(dictionary, token, project_name):
    """
    Returns the number of commits for a given project
    
    Input: dictionary that contains repository information
    """
    commits_url = dictionary["commits_url"] # returns of form 'https://api.github.com/repos/sindrets/diffview.nvim/commits{/sha}'
    # remove suffix so it can be used for api call
    try:
        commits_url = commits_url[0:-6] 
    except Exception as e:
        print(e)
    
    # issue request
    headers = {'Authorization': f'token {token}'}
    try:
        r = requests.get(commits_url, headers=headers)
        r = r.json()
    except Exception as e:
        print(e) 
    num_commits = len(r) # length of this list correspons to the number of commits

    output = json.dumps({project_name: num_commits})
    producer_2.send((output).encode('utf_8'))
    

def get_unit_tests(dictionary, headers, language):
    """ TODO """
    query_url3 = dictionary["contents_url"][0:-7] 
    req = requests.get(query_url3 , headers=headers)
    for item in req.json():
        if('test' in item['name']):
            # send to producer
            producer_3.send((language).encode('utf_8'))
            return True, query_url3
    return False, query_url3
        
        
def get_continuous_integration(dictionary, query_url3, headers, language):
    # https://docs.github.com/en/actions/learn-github-actions/understanding-github-actions
    
    query_url4 = query_url3+".github/workflows"
    req = requests.get(query_url4 , headers=headers)
    #if it hasnt have workflow directory, then it will return message not found, otherwise it
    #will return the object with all the items in such directory (and the indexs will be integer)
    try:
        # message: "not found"
        # in this case the workflow directory doesn't exist
        req.json()["message"]
    except TypeError as e:
        # if it exists, there is no message, i.e., TypeError
        # send it to producer
        producer_4.send((language).encode('utf_8'))

    

def query_github(start_date: datetime, num_days: int, tokens: list):
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
                        # Q1 programming languages
                        language = get_programming_language(dictionary)
                        
                        if language is None:
                            continue
            
                        # Q2 nmber of commits of project
                        get_num_commits(dictionary, headers, project_name=dictionary["name"]) 
                        
                        #Q3 unit tests                      
                        has_test, query_url3 = get_unit_tests(dictionary, headers,language)
                        #Q4 CI/CD
                        if(has_test):                          
                            get_continuous_integration(dictionary, query_url3, headers,language)

                except KeyError as e:
                    print(e)                    
                    print('KeyError when selecting "items"')

        # increment day
        curr_date += datetime.timedelta(days=1)
        print(curr_date)
        

if __name__=="__main__":
    # get github tokens
    tokens = get_tokens(["githubtoken_jonas.txt"])
    
    start = time.time()
    # query github for next 3 days
    query_github(datetime.date(2021, 5, 1), 1, tokens)
    end = time.time()
    print('duration: ', end-start)