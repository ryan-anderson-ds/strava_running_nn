#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 17:43:21 2019
@author: rian-van-den-ander
"""

"""

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
WARNING
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

DO NOT USE THIS ONCE SITE HAS INGESTED ATHLETES, SINCE FILES WILL BE NEW


"""

import pandas as pd 
import glob
from feature_engineer_athlete import feature_engineer
from sql_methods import write_db_replace, read_db

metadata_athletes = pd.DataFrame()
metadata_blocks = pd.DataFrame()
all_athlete_activities = pd.DataFrame()
all_athlete_weeks = pd.DataFrame()
features_activities = pd.DataFrame()
features_weeks = pd.DataFrame()
features_blocks = pd.DataFrame()
regressors = dict()
average_paces_and_hrs = pd.DataFrame()

path = './data/*.txt'   
files=glob.glob(path)   

processing_status = pd.DataFrame()
count = 0

"""
!!!!!!!!!!!
IF I NEED THIS AGAIN, HAVE TO PASS THROUGH ATHLETE DATA FROM FILE

here's a snippet doing a bit of that

    if(athlete_id != ''):
        file = './data/' + str(athlete_id) + '.txt'  
    
    f=open(file, 'r', encoding="utf8")  
    file_data = f.read()   
    dict_data = ast.literal_eval(file_data)
    f.close()        

!!!!!!!!!!



for file in files:     
    
    athlete_id = file[7:-4] #for this workflow, i must get athlete id from filename

    if count > 0:    
        processing_status = read_db('processing_status')     
        
    processing_status = processing_status.append({'athlete_id': athlete_id,
                                                  'status': 'none',
                                                  'bearer_token': '',
                                                  'refresh_token': ''}, ignore_index = True)   
    
    write_db_replace(processing_status, 'processing_status')
    
    feature_engineer(file=file) 
"""

