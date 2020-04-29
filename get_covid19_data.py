import pandas as pd
import os
import git
import sqlite3
import time

def covid19_data_preperation():
    # modify_states_name
    # file name is changed from states.csv to states-name.csv, US Countiess.csv to counties-name.csv
    if not os.path.exists('states-name-r.csv'):
        print('Loading states name from origin...')
        states_name = pd.read_csv('states-name.csv')
        states_add = pd.DataFrame([['American Samoa', 'AS'], 
                                ['Guam', 'GU'],
                                ['Northern Mariana Islands', 'MP'],
                                ['Puerto Rico', 'PR'],
                                ['Virgin Islands', 'VI']], columns=['State', 'Abbreviation'])
        states_name = states_name.append(states_add, sort=True, ignore_index=True)
        states_name.to_csv('states-name-r.csv')
    else:
        print('Loading states name using cache...')
        states_name = pd.read_csv('states-name-r.csv')


    # modify_counties_name
    if not os.path.exists('counties-name-r.csv'):
        print('Loading counties name from origin...')
        counties_name = pd.read_csv('counties-name.csv')
        counties_name = counties_name.rename(columns={'X1': 'Abbreviation', 'X4': 'County'})
        counties_add = pd.DataFrame({"County": ['New York City', 'Kansas City'], 
                                    "Abbreviation": ['NY', 'MO']})
        
        counties_name['State'] = ''
        counties_name['StateId'] = ''
        counties_name = counties_name.append(counties_add, sort=True, ignore_index=True)
        Abb_index = pd.Index(states_name['Abbreviation'])
        for i in range(counties_name.shape[0]):
            if counties_name.loc[i, 'Abbreviation'] in states_name['Abbreviation'].values:
                ind = Abb_index.get_loc(counties_name['Abbreviation'][i])
                counties_name.loc[i, 'State'] = states_name['State'][ind]
                counties_name.loc[i, 'StateId'] = ind + 1
                counties_name.loc[i, 'County'] = counties_name.loc[i, 'County'].replace(' County', '')
                counties_name.loc[i, 'County'] = counties_name.loc[i, 'County'].replace(' Parish', '')
                counties_name.loc[i, 'County'] = counties_name.loc[i, 'County'].replace(' Municipality', '')
                counties_name.loc[i, 'County'] = counties_name.loc[i, 'County'].replace('Dona Ana', 'Doña Ana')
                counties_name.loc[i, 'County'] = counties_name.loc[i, 'County'].replace('Petersburg Census Area', 'Petersburg Borough')
                if counties_name.loc[i, 'Abbreviation'] == 'SD':
                    counties_name.loc[i, 'County'] = counties_name.loc[i, 'County'].replace('Shannon', 'Oglala Lakota')
            else:
                counties_name = counties_name.drop([i])
        counties_name.to_csv('counties-name-r.csv')
    else:
        print('Loading counties name using cache...')
        counties_name = pd.read_csv('counties-name-r.csv')



    # get the data files from github
    if not os.path.exists('FP_DATA'):
        print('Loading us covid-19 data from origin...')
        git.Repo.clone_from(url='https://github.com/nytimes/covid-19-data.git', to_path='FP_DATA')
        repo = git.Repo('FP_DATA')
        repo.remotes.origin.pull()

        us = pd.read_csv('FP_DATA/us.csv')

        us_states = pd.read_csv('FP_DATA/us-states.csv')
        State_index = pd.Index(states_name['State'])
        us_states['stateid'] = ''
        for i in range(us_states.shape[0]):
            ind = State_index.get_loc(us_states['state'][i])
            us_states.loc[i, 'stateid'] = ind + 1
        us_states.to_csv('FP_DATA/us-states.csv')

        us_counties = pd.read_csv('FP_DATA/us-counties.csv')
        County_index = pd.Index(zip(counties_name['State'], counties_name['County']))
        us_counties['countyid'] = ''
        for i in range(us_counties.shape[0]):
            if us_counties['county'][i] == 'Unknown':
                us_counties.loc[i, 'countyid'] = -1
                continue
            if us_counties['state'][i] == 'Louisiana' and us_counties['county'][i] == 'LaSalle':
                us_counties.loc[i, 'county'] = 'La Salle'

            ind = County_index.get_loc((us_counties['state'][i], us_counties['county'][i]))
            us_counties.loc[i, 'countyid'] = ind + 1
        us_counties.to_csv('FP_DATA/us-counties.csv')
    else:
        print('Loading us covid-19 data using cache...')
        us = pd.read_csv('FP_DATA/us.csv')
        us_states = pd.read_csv('FP_DATA/us-states.csv')
        us_counties = pd.read_csv('FP_DATA/us-counties.csv')

    # import_to_sql
    if not os.path.exists("FP_DATA/us_covid19.sqlite"):
        print('Loading us covid-19 data from csv to sql...')
        conn = sqlite3.connect("FP_DATA/us_covid19.sqlite")
        cur = conn.cursor()

        drop_states_name = '''
            DROP TABLE IF EXISTS "states_name";
        '''
        create_states_name = '''
            CREATE TABLE IF NOT EXISTS "states_name" (
                "id" INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                "state"  TEXT NOT NULL
            )
        '''
        cur.execute(drop_states_name)
        cur.execute(create_states_name)
        for i in range(states_name.shape[0]):
            insert_states_name = '''INSERT INTO states_name VALUES (NULL, ?)'''
            insert_value = list(states_name.loc[i,['State']])
            cur.execute(insert_states_name, insert_value)
        conn.commit()
        #####
        drop_counties_name = '''
            DROP TABLE IF EXISTS "counties_name";
        '''
        create_counties_name = '''
            CREATE TABLE IF NOT EXISTS "counties_name" (
                "id" INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                "county"  TEXT NOT NULL,
                "stateid" INTEGER NOT NULL,
                FOREIGN KEY ("stateid") REFERENCES states_name("id") 
            )
        '''
        cur.execute(drop_counties_name)
        cur.execute(create_counties_name)
        for i in range(counties_name.shape[0]):
            insert_counties_name = '''INSERT INTO counties_name VALUES (NULL, ?, ?)'''
            insert_value = list(counties_name.loc[i,['County','StateId']])
            insert_value = [insert_value[0], int(insert_value[1])]
            cur.execute(insert_counties_name, insert_value)
        conn.commit()
        #####
        us = pd.read_csv('FP_DATA/us.csv')
        us.to_sql('us', conn, if_exists='replace', index = False) 
        #####
        drop_us_states = '''
            DROP TABLE IF EXISTS "us_states";
        '''

        create_us_states = '''
            CREATE TABLE IF NOT EXISTS "us_states" (
                "date" TEXT NOT NULL,
                "stateid" INTEGER NOT NULL,
                "cases" INTEGER,
                "deaths" INTEGER,
                FOREIGN KEY ("stateid") REFERENCES states_name("id")

            )
        '''
        cur.execute(drop_us_states)
        cur.execute(create_us_states)
        for i in range(us_states.shape[0]):
            insert_us_states = '''INSERT INTO us_states VALUES (?, ?, ?, ?)'''
            insert_value = list(us_states.loc[i,['date','stateid','cases','deaths']])
            insert_value = [insert_value[0], int(insert_value[1]), int(insert_value[2]), int(insert_value[3])]
            cur.execute(insert_us_states, insert_value)
        conn.commit()
        #####
        drop_us_counties = '''
            DROP TABLE IF EXISTS "us_counties";
        '''

        create_us_counties = '''
            CREATE TABLE IF NOT EXISTS "us_counties" (
                "date" TEXT NOT NULL,
                "countyid" INTEGER NOT NULL,
                "cases" INTEGER,
                "deaths" INTEGER,
                FOREIGN KEY ("countyid") REFERENCES counties_name("id")
            )
        '''
        cur.execute(drop_us_counties)
        cur.execute(create_us_counties)
        for i in range(us_counties.shape[0]):
            insert_us_counties = '''INSERT INTO us_counties VALUES (?, ?, ?, ?)'''
            insert_value = list(us_counties.loc[i,['date','countyid','cases','deaths']])
            insert_value = [insert_value[0], int(insert_value[1]), int(insert_value[2]), int(insert_value[3])]
            cur.execute(insert_us_counties, insert_value)
        conn.commit()

    else:
        print('Loading us covid-19 data from existing sql...')

if __name__ == '__main__':
    covid19_data_preperation()
