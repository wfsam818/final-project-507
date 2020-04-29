import time
import os
import git
import praw
import json
import requests
import sqlite3
import pandas as pd
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import webbrowser
import secrets
import get_covid19_data

def load_cache():
    try:
        cache_file = open('FP_DATA/FPcache.json', 'r')
        cache_file_contents = cache_file.read()
        cache = json.loads(cache_file_contents)
        cache_file.close()
    except:
        cache = {}
    return cache


def save_cache(cache):
    cache_file = open('FP_DATA/FPcache.json', 'w')
    contents_to_write = json.dumps(cache)
    cache_file.write(contents_to_write)
    cache_file.close()


def construct_covid19_data_state(state):
    conn = sqlite3.connect('FP_DATA/us_covid19.sqlite')  
    cur = conn.cursor()
    query = f'''
    SELECT date, cases, deaths
    FROM us_states
        JOIN states_name ON us_states.stateid = states_name.id
    WHERE state = '{state}'
    '''
    cur.execute(query)
    state_date = list()
    state_cases = list()
    state_deaths = list()
    for row in cur:
        state_date.append(row[0])
        state_cases.append(row[1])
        state_deaths.append(row[2])
    conn.close()
    return state_date, state_cases, state_deaths


def construct_covid19_data_county(state, county):
    conn = sqlite3.connect('FP_DATA/us_covid19.sqlite')  
    cur = conn.cursor()
    query = f'''
    SELECT date, cases, deaths
    FROM us_counties
        JOIN counties_name ON us_counties.countyid = counties_name.id
            JOIN states_name ON counties_name.stateid = states_name.id
    WHERE state = '{state}' AND county = '{county}'
    '''
    cur.execute(query)
    county_date = list()
    county_cases = list()
    county_deaths = list()
    for row in cur:
        county_date.append(row[0])
        county_cases.append(row[1])
        county_deaths.append(row[2])
    conn.close()
    return county_date, county_cases, county_deaths


def construct_reddit_data(state):
    # OATH2 
    reddit = praw.Reddit(client_id=secrets.Reddit_Client_ID,
                        client_secret=secrets.Reddit_Client_Secret,
                        user_agent=secrets.Reddit_user_agent,
                        username=secrets.Reddit_username,
                        password=secrets.Reddit_password)
    unique_key = 'reddit_' + state

    if (unique_key in FPcache.keys()): 
        print("Using cache...")
        return FPcache[unique_key]    
    else:
        print("Fetching...")
        all_subreddit = reddit.subreddit('all')
        top_subreddit = all_subreddit.search(f'{state} coronavirus', sort='top', limit=10)
        top_list = []
        top_score = []
        top_url = []
        for ts in top_subreddit:
           top_list.append(ts.title)
           top_score.append(ts.score)
           top_url.append(ts.url)
        FPcache[unique_key] = {'list': top_list, 'score': top_score, 'url': top_url}
        save_cache(FPcache)   
        return FPcache[unique_key]


def construct_yelp_data(state, county):
    headers = {'Authorization': 'Bearer %s' % secrets.Yelp_API_Key}
    base_url = 'https://api.yelp.com/v3/businesses/search'
    params1 = {
        "term": "takeout",
        "location": ' '.join([state, county]),
        "offset": 0,
        "limit": 50,
        "sort_by": "rating",
        "open_now": True}
    params2 = {
        "term": "takeout",
        "location": ' '.join([state, county]),
        "offset": 50,
        "limit": 50,
        "sort_by": "rating",
        "open_now": True}   
    unique_key = 'yelp_' + '_'.join([state, county])
    if (unique_key in FPcache.keys()): 
        print("Using cache...")
        return FPcache[unique_key]    
    else:
        print("Fetching...")
        response1 = requests.get(base_url, params=params1, headers=headers)
        response_json1 = response1.json()
        response2 = requests.get(base_url, params=params2, headers=headers)
        response_json2 = response2.json()
        business_jsons = response_json1['businesses'] + response_json2['businesses']
        yelp_name = list()
        yelp_rating = list()
        yelp_price = list()
        yelp_url = list()
        for i in business_jsons:
            yelp_name.append(i['name'])
            yelp_rating.append(i['rating'])
            yelp_url.append(i['url'])
            try:
                yelp_price.append(i['price'])
            except:
                yelp_price.append('no info')
        FPcache[unique_key] = {'name': yelp_name, 'rating': yelp_rating, 'url': yelp_url, 'price': yelp_price}
        save_cache(FPcache)   
        return FPcache[unique_key]

def plotly_bar_plot(xvals, yvals, ort, text, title, nm):   
    bar_data = go.Bar(x=xvals, y=yvals, orientation=ort, hovertext=text)
    basic_layout = go.Layout(title=title)
    fig = go.Figure(data=bar_data, layout=basic_layout)
    fig.write_html(nm, auto_open=True)


def plotly_plot(xvals1, yvals1, xvals2, yvals2, location, nm):
    fig = make_subplots(rows=1, cols=2, subplot_titles=('cases in '+location, 'deaths in '+location))

    bar_data = go.Bar(x=xvals1, y=yvals1)
    fig.add_trace(bar_data, row=1, col=1)

    scatter_data = go.Scatter(x=xvals2, y=yvals2)
    fig.add_trace(scatter_data, row=1, col=2)

    fig.update_layout(showlegend=False, title_text='COVID-19 in '+location)

    fig.write_html(nm, auto_open=True)


def plotly_pie_plot(labels, values, location, num, nm):
    pie_data = go.Pie(labels=labels, values=values)
    basic_layout = go.Layout(title=f'percentage of best {num} restaurants open now (sort by Bayesian rating) in price categories in {location}')
    fig = go.Figure(data=pie_data, layout=basic_layout)
    fig.write_html(nm, auto_open=True)


def get_states():
    conn = sqlite3.connect('FP_DATA/us_covid19.sqlite')  
    cur = conn.cursor()
    query = f'''
    SELECT state
    FROM states_name
    '''
    cur.execute(query)
    states_name_list = list()
    for row in cur:
        states_name_list.append(row[0])
    return states_name_list

def get_counties(state):
    conn = sqlite3.connect('FP_DATA/us_covid19.sqlite')  
    cur = conn.cursor()
    query = f'''
    SELECT county
    FROM counties_name
        JOIN states_name ON counties_name.stateid = states_name.id
    WHERE state = '{state}'
    '''
    cur.execute(query)
    counties_name_list = list()
    for row in cur:
        counties_name_list.append(row[0])
    return counties_name_list


if __name__ == "__main__":
    # for updating data
    now_time = time.time()
    create_time = os.path.getctime('FP_DATA')
    if now_time - create_time > 24*3600:
        os.remove('FP_DATA')
    # for loading data
    get_covid19_data.covid19_data_preperation()
    FPcache = load_cache()
    input_value = ''
    while input_value != 'exit':
        input_value = input('\n\nPlease enter the full name of a US State/Federal District/Territory\nor enter exit: ')
        input_value = input_value.strip()
        states_list = get_states()
        states_list_lower = list(map(str.lower, get_states()))
        if input_value == 'exit':
            print('Thank you for using our program')
            break
        elif input_value.lower() not in states_list_lower:
            print('Please enter a valid US State/Federal District/Territory')
            continue
        else:
            input_state = input_value
            print(f'Data of cases and deaths due to COVID-19 in {input_state} will shown in two plots')
            state_index = states_list.index(input_state)
            input_state = states_list[state_index]
            state_date, state_cases, state_deaths = construct_covid19_data_state(input_state)
            plotly_plot(state_date, state_cases, state_date, state_deaths, input_state, 'plot1.html')
            top_ten = construct_reddit_data(input_state)
            y_list = list(map(lambda x: 'Top '+str(x), list(range(1,11))))[::-1]
            plotly_bar_plot(top_ten['score'][::-1], y_list, 'h', top_ten['list'][::-1], 
                            f'Scores of top 10 discussions about COVID-19 in {input_state}', 'plot2.html')
            while input_value != 'exit':
                input_value = input('\n\nEnter a number to choose a dicussion you interest\nor enter exit/back/skip: ')
                top_url = top_ten['url']
                if input_value == 'exit':
                    print('Thank you for using our program')
                    break
                elif input_value == 'back':
                    print('Back to last step')
                    break
                elif not input_value.isnumeric() and input_value != 'skip':
                    print(f'Please enter a valid integer in the range of 1 to {len(top_url)}')
                    continue
                elif input_value.isnumeric() and (int(input_value) not in list(range(1, len(top_url)+1))):
                    print(f'Please enter a valid integer in the range of 1 to {len(top_url)}')
                    continue
                elif input_value.isnumeric() and (int(input_value) in list(range(1, len(top_url)+1))):
                    input_reddit = input_value
                    webbrowser.open(top_url[int(input_reddit)-1])
                elif input_value == 'skip':
                    counties_list = get_counties(input_state)
                    counties_list_lower = list(map(str.lower, get_counties(input_state)))
                    while input_value != 'exit':
                        if input_state in ['American Samoa', 'Guam', 'Northern Mariana Islands', 'Puerto Rico', 'Virgin Islands']:
                            print('There is no county level data for the 5 territories of US')
                            input_county = input_state
                        else:
                            input_value = input('\n\nPlease enter a County/Parish/Municipality in the Area you just chose\nor enter exit/back: ')
                            if input_value == 'exit':
                                print('Thank you for using our program')
                                break
                            elif input_value == 'back':
                                print('Back to last step')
                                break
                            elif input_value.lower() not in counties_list_lower:
                                print('Please enter a valid County/Parish/Municipality in the area')
                                continue
                            else:
                                input_county = input_value
                                print(f'Data of cases and deaths due to COVID-19 in {input_county}, {input_state}will shown in two plots')
                                county_index = counties_list.index(input_county)
                                input_county = counties_list[county_index]
                                county_date, county_cases, county_deaths = construct_covid19_data_county(input_state, input_county)
                                if county_date == []:
                                    print(f'Data of {input_county} is unavailable')
                                else:
                                    plotly_plot(county_date, county_cases, county_date, county_deaths, input_value, 'plot3.html')
                        print('In this COVID-19 Pandemic Time, please grab good food, keep healthy and keep social distance!')
                        food_info = construct_yelp_data(input_state, input_county)
                        price_cat = sorted(list(set(food_info['price'])))
                        price_num = list()
                        for i in price_cat:
                            price_num.append(food_info['price'].count(i))
                        plotly_pie_plot(price_cat, price_num, input_county, len(food_info['price']), 'plot4.html')
                        
                        while input_value != 'exit':
                            input_value = input('\n\nEnter a price (dollar signs) in the pie plot to choose a restaurant\nor enter exit/back: ')
                            if input_value == 'exit':
                                print('Thank you for using our program')
                                break
                            elif input_value == 'back':
                                print('Back to last step')
                                break
                            elif input_value not in price_cat:
                                print('Please enter a valid price in dollar signs or enter no info')
                                continue
                            else:
                                input_price = input_value
                                food_price = food_info['price']
                                food_name = food_info['name']
                                food_rating = food_info['rating']
                                food_name_sub = [v for i,v in enumerate(food_name) if food_price[i]==input_price]
                                food_rating_sub = [v for i,v in enumerate(food_rating) if food_price[i]==input_price]
                                y_list = list(map(lambda x: 'No. ' + str(x), list(range(1, len(food_name_sub)+1))))[::-1]
                                plotly_bar_plot(food_rating_sub[::-1], y_list, 'h', food_name_sub[::-1], 
                                                f'Price {input_price} restaurants open now (sort by Bayesian rating) and their real ratings in {input_county}', 'plot5.html')
                                while input_value != 'exit':
                                    input_value = input('\n\nEnter a number to choose a retaurant\nor enter exit/back: ')
                                    food_url = food_info['url']
                                    food_url_sub = [v for i,v in enumerate(food_url) if food_price[i]==input_price]
                                    if input_value == 'exit':
                                        print('Thank you for using our program')
                                        break
                                    elif input_value == 'back':
                                        print('Back to last step')
                                        break
                                    elif not input_value.isnumeric():
                                        print(f'Please enter a valid integer in the range of 1 to {len(food_name_sub)}')
                                        continue
                                    elif input_value.isnumeric() and (int(input_value) not in list(range(1, len(food_name_sub)+1))):
                                        print(f'Please enter a valid integer in the range of 1 to {len(food_name_sub)}')
                                        continue
                                    else:
                                        input_yelp = input_value
                                        webbrowser.open(food_url_sub[int(input_yelp)-1])