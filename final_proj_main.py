import secrets
import os
import git
import praw
import json
import requests
import sqlite3
import pandas as pd

def load_cache():
    try:
        cache_file = open('FPcache.json', 'r')
        cache_file_contents = cache_file.read()
        cache = json.loads(cache_file_contents)
        cache_file.close()
    except:
        cache = {}
    return cache

def save_cache(cache):
    cache_file = open('FPcache.json', 'w')
    contents_to_write = json.dumps(cache)
    cache_file.write(contents_to_write)
    cache_file.close()


def construct_covid19_data():
    # get the data files from github
    if not os.path.exists('COVID19'):
        git.Repo.clone_from(url='https://github.com/nytimes/covid-19-data.git', to_path='COVID19')
    repo = git.Repo('COVID19')
    repo.remotes.origin.pull()

    # change csv to sqlite
    conn = sqlite3.connect('covid.db')  
    conn.cursor()

    us = pd.read_csv('COVID19/us.csv')
    us.to_sql('us', conn, if_exists='replace', index = False) 

    us_states = pd.read_csv ('COVID19/us-states.csv')
    us_states.to_sql('us-states', conn, if_exists='replace', index = False) 

    us_states = pd.read_csv ('COVID19/us-counties.csv')
    us_states.to_sql('us-counties', conn, if_exists='replace', index = False) 


def construct_reddit_data(state):
    # OATH2 
    reddit = praw.Reddit(client_id=secrets.Reddit_Client_ID,
                        client_secret=secrets.Reddit_Client_Secret,
                        user_agent=secrets.Reddit_user_agent,
                        username=secrets.Reddit_username,
                        password=secrets.Reddit_password)
    unique_key = state
    # return the top sub reddits
    all_subreddit = reddit.subreddit('all')
    top_subreddit = all_subreddit.search(f'{state} coronavirus', sort='top', limit=10)
    return top_subreddit
    # for ts in top_subreddit:
    #    print(ts)


def construct_yelp_data(location):
    headers = {'Authorization': 'Bearer %s' % secrets.Yelp_API_Key}
    base_url = 'https://api.yelp.com/v3/businesses/search'
    params = {
        "term": "takeout",
        "location": location,
        "limit": 50}
    
    para_list = []
    for k in params.keys():
        para_list.append(f'{k}_{params[k]}')
    para_list.sort()
    unique_key = base_url + '_' +  '_'.join(para_list)

    if (unique_key in FPcache.keys()): 
        print("Using cache")
        return FPcache[unique_key]    
    else:
        print("Fetching")
        response = requests.get(base_url, params=params, headers=headers)
        response_json = response.json()
        cache[uni_keys] = response_json
        save_cache(FPcache)   
        return FPcache[unique_key]


if __name__ == "__main__":
    FPcache = load_cache()
    construct_covid19_data()