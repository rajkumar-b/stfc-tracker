import pymysql
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# MySQL configuration
host = '127.0.0.1'
user = 'root'
password = 'abc123'
database = 'stfc'

# Connect to the database
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')


# Gets data for a single page for the given STFC server
def get_player_data(page_no: int = 0, server_no: int = 100):
    # API URL to repeatedly get data in loop by providing page no
    url = f"https://stfc.wtf/power/__data.json?sort=score&server={server_no}&page={page_no}"
    data = json.loads(requests.get(url).content)  # Get data from specified page
    nodes = data['nodes'][1]['data']  # actual data (all)
    nodes_data = nodes[0]  # high level configs like total count, per page count and player-wise data reside here
    total_count = nodes[nodes_data['count']]
    per_page_count = nodes[nodes_data['perPage']]
    players = nodes[nodes_data['players']]
    # Convert gibberish data into proper format
    player_data = []
    for player_idx in players:
        player = nodes[player_idx]
        for k, v in player.items():
            player[k] = nodes[v]
        player_data.append(player)
    # A check that marks if any more data is still pending for retrieval
    has_more_data = total_count > per_page_count * (page_no + 1)
    # sends back player data, and to loop more if necessary
    return player_data, has_more_data


# Gets data for given server page by page and accumulates them. Retry count is added for fail safely.
def accumulated_server_data(retries=10, server: int = 100):
    # The server might not respond sometimes, so retry for given no of times if all data is not retrieved in first try
    player_data = []
    for i in range(retries + 1):
        player_data.clear()  # re-initializing for subsequent retries so old data is cleared
        page_no = 0
        has_data = True
        try:
            # Keep looping till there is no more data for retrieval
            while has_data:
                page_data, has_data = get_player_data(page_no, server)
                player_data.extend(page_data)
                page_no += 1
        except Exception as e:
            print(f"Retrying post timeout, count = {i + 1}")
        if page_no > 10:
            break
    return player_data


# Custom formatting data to understand each keys
def format_data(data):
    return {
        'player': data['id'],
        'server_no': int(data['server']),
        'alliance': data['alliance'],
        'ops': int(data['level']),
        'mission_count': -1 if not data['mcomplete'] else int(data['mcomplete']),
        'qs_assessment': -1 if not data['ass'] else int(data['ass']),
        'alliance_help': -1 if not data['ahelp'] else int(data['ahelp']),
        'rss_raided': -1 if not data['rss'] else int(data['rss']),
        'rss_mined': -1 if not data['RSSmined'] else int(data['RSSmined']),
        'curr_power': -1 if not data['score'] else int(data['score']),
        'power_destroyed': -1 if not data['pd'] else int(data['pd']),
        'pvp_wins': -1 if not data['PDestroyed'] else int(data['PDestroyed']),
        'pvp_damage_done': -1 if not data['PDamaged'] else int(data['PDamaged']),
        'pve_wins': -1 if not data['HDestroyed'] else int(data['HDestroyed']),
        'pve_damage_done': -1 if not data['HDamage'] else int(data['HDamage']),
        'kdr': -1.00 if not data['KDR'] else round(float(data['KDR']), 2)
    }


# Logic to track only necessary values that needs to be monitored. data is supposed to have _past and _curr data.
def get_player_activity(data: pd.DataFrame):
    for past_col in data.columns:
        if '_past' in past_col:
            col = str(past_col).replace('_past', '')
            curr_col = f"{col}_curr"
            # Mark activity as 1 if there is change, otherwise 0.
            data[col] = data.apply(lambda x: '1' if x[curr_col] != x[past_col] else '0', axis=1)
            # A special case to track if someone is raiding actively. Can be used for incursions with diff server no.
            # But real problem is, data is not updated in real time. This logic is useless as of now for incursion.
            if 'rss_raided' in col:
                data['rss_change'] = data[curr_col] - data[past_col]
    # Data restructuring to include only activity data and remove past/current data values
    reqrd_cols = [c for c in data.columns if '_curr' not in c and '_past' not in c] + ['ops_curr', 'alliance_curr']
    new_data = data[reqrd_cols].copy()
    activity_cols = ['ops', 'mission_count', 'qs_assessment', 'alliance_help', 'rss_raided', 'rss_mined', 'curr_power',
                     'power_destroyed', 'pvp_wins', 'pvp_damage_done', 'pve_wins', 'pve_damage_done', 'kdr']
    # Club all activity into single column so it can be converted into a single int from 1's and 0's
    new_data['activity_byte'] = new_data.apply(lambda x: int(''.join([x[y] for y in activity_cols]), 2), axis=1)
    # Now all the activity columns are obsolete, no need to include them anymore as long as the logic is known
    # as to how to interpret the int data to binary, then map to respective columns in above order
    new_data.drop(columns=activity_cols, inplace=True)
    new_data.rename(columns={'alliance_curr': 'alliance', 'ops_curr': 'ops'}, inplace=True)
    # Include current timestamp as well in the data
    new_data['qtstp'] = int(datetime.now().timestamp())
    # Return only the data of ppl who are active. Ignore others.
    return new_data[new_data['activity_byte'] > 0]


def lambda_handler(event, context):
    try:
        # Get past data from MySQL
        past_data = pd.read_sql_query("SELECT * FROM player_data where server_no = 100;", engine)
        # Get current data from API
        curr_data = pd.DataFrame([format_data(data) for data in accumulated_server_data()])
        # Check if there is any change. If not, it essentially means the API does not have update yet
        if curr_data.equals(past_data):
            print("No change")
        else:
            # Since there is some change, merge old and new data into one with suffixes.
            merge_data = pd.merge(curr_data, past_data, 'left', on=['player', 'server_no'], suffixes=['_curr', '_past'])
            merge_data.drop(columns=['alliance_past'], inplace=True)
            activity_data = get_player_activity(merge_data)  # Get activity data
            # print(activity_data.head())  # Print first five data to verify, Optional step.
            # Below is activity tracking for incursion. Useless for now.
            s99_data = pd.DataFrame([format_data(data) for data in accumulated_server_data(server=99)])
            final_data = pd.concat([curr_data, s99_data], axis=0, ignore_index=True)
            # Upload both player data and activity data to MySQL tables
            final_data.to_sql(name='player_data', con=engine, if_exists='replace', index=False)
            activity_data.to_sql(name='player_activity', con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error putting item:", e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error storing data')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Data stored successfully')
    }


if __name__ == '__main__':
    lambda_handler(None, None)
