#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 10 18:08:59 2019
@author: rian-van-den-ander
"""

""" 
Basics:
    
To get data on athletes, you will have to make an application and request that
athletes sign in with Strava, and grant your application certain permission
using OAuth 2.0. You can get data on yourself without authentication for testing purposes.

Strava API usage is limited on a per-application basis using both a 
15-minute and daily request limit. 
The default rate limit allows 
- 600 requests every 15 minutes
- with up to 30,000 requests per day. 
- aka 40 req/min for 12h 
- to a maximum of 6 hours per access token
- this script uses 1800 requests for me, an athlete with many activities. 
so one ingestion per day is possible, worst case

1. It seems like I found a bug in the API - if you request the wrong access token, and re-request
with different levels of access, it still keeps the previous levels.


"""

import requests

"""
ACCESS PARAMETERS
-----------------
Required scopes: scope=read,activity:read,profile:read_all
From: https://www.strava.com/oauth/authorize?client_id=40695&response_type=code&redirect_uri=http://localhost/exchange_token&approval_prompt=force&scope=activity:read,profile:read_all

"""

client_id = '40695' #unique identifier for app
from sql_methods import read_db, write_db_replace

"""
GET ATHLETE
-----------
"""
def get_athlete(bearer_token):
    
    url = 'https://www.strava.com/api/v3/athlete'
    data = ''
    headers = {"Authorization": "Bearer " + bearer_token}
    response = requests.get(url, data=data, headers=headers)
    athlete_data = response.json()
    
    try:
        athlete_id = athlete_data['id']
        print(str(athlete_id))
    except Exception:
        print ("Error requesting athlete data from Strava")
        return 1
    
    return athlete_data

def get_athlete_data_status(athlete_id):

    import pandas as pd
    
    processing_status = read_db('processing_status')
    
    if str(athlete_id) in processing_status["athlete_id"].values:        
        ingest_status = processing_status[processing_status["athlete_id"] == str(athlete_id)]["status"].values[0]
        return ingest_status
    
    return "to process"

def queue_athlete_for_processing(athlete_id, bearer_token, refresh_token):

    import pandas as pd  
      
    processing_status = read_db('processing_status')
    processing_status = processing_status.append({'athlete_id': athlete_id,
                                                  'status': 'none',
                                                  'bearer_token': bearer_token,
                                                  'refresh_token': refresh_token}, ignore_index = True)
    write_db_replace(processing_status, 'processing_status')           
    
    return "none"

