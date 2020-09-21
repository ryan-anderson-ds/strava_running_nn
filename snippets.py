# -*- coding: utf-8 -*-
"""
Created on Tue Dec 31 19:16:48 2019

@author: rian-van-den-ander
"""

"""
CHECK PROCESSING STATUS AND THEN PROCESS ALL ATHLETES
"""

from sql_methods import read_db, write_db_replace, write_db_insert
processing_status = read_db('processing_status')

"""
RYAN: FOR NOW, MANUALLY REFRESH TOKENS - something is funky there
"""
from cron_update_data import refresh_tokens
refresh_tokens()    

#process (FROM STRAVA API) all 'none' athletes
from cron_update_data import update_data
from cron_train_nn import cron_train_nn
update_data()
cron_train_nn()


"""
DELETE AN ATHLETE
"""

import pandas as pd
from sql_methods import read_db, write_db_replace, write_db_insert
from cron_train_nn import cron_train_nn

processing_status = read_db('processing_status')

"""
DELETE SOMEONE FROM PROCESSING STATUS
"""
processing_status = read_db('processing_status')
processing_status = processing_status.drop(21)
write_db_replace(processing_status, 'processing_status')



"""
UPDATE SPECIFIC ROW
"""
processing_status.at[19,"status"] = 'processed'
write_db_replace(processing_status, 'processing_status')


"""
Get emails to send AND DELETE THE EMAILS DB
"""
emails = read_db('emails')

from sql_methods import delete_rows
delete_rows('emails')


"""
POPULATE ALL FROM FILES
"""
processing_status = read_db('processing_status')

metadata_athletes = pd.DataFrame()
metadata_blocks = pd.DataFrame()
all_athlete_activities = pd.DataFrame()
all_athlete_weeks = pd.DataFrame()
features_activities = pd.DataFrame()
features_weeks = pd.DataFrame()
features_blocks = pd.DataFrame()
average_paces_and_hrs = pd.DataFrame()

from sql_methods import delete_rows
delete_rows('metadata_athletes')
delete_rows('metadata_blocks')
delete_rows('all_athlete_activities')
delete_rows('all_athlete_weeks')
delete_rows('features_activities')
delete_rows('features_weeks')
delete_rows('features_blocks')
delete_rows('average_paces_and_hrs')
    
from feature_engineer_athlete import feature_engineer

processing_status = read_db('processing_status')
for index,row in processing_status.iterrows():
    
    athlete_id = int(row['athlete_id'])
    print(athlete_id)
    feature_engineer(athlete_id,'', populate_all_from_files=1)
