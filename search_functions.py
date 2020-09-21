# -*- coding: utf-8 -*-
"""
Created on Sun Dec  8 14:41:49 2019
@author: rian-van-den-ander
"""

def get_activity(activities, activity_id):    
        for activity in activities:
            if int(activity['id']) == int(activity_id):
                return activity
           

def get_block(activities, activity_date, duration_days = 91):
    
    """
    NB: Assumes activities are in chronological order
    """
    import datetime
    
    block_activities = []
    
    
    for activity in activities:
        
        #catch invalid activities
        try:
            activity['type']    
        except Exception:
            continue
        
        current_traverse_date = datetime.datetime.strptime(activity['start_date'][:10], '%Y-%m-%d')
        
        time_diff = (activity_date-current_traverse_date).days
        if(time_diff < duration_days and time_diff >= 0):
            block_activities.append(activity)
            
        

    return block_activities


def get_weeks(block_activities, duration_days = 91):
    
    """
    NB: Assumes activities are in chronological order
    """
    from datetime import datetime, timedelta
    import math
    from itertools import dropwhile 
    
    end_date = datetime.strptime(block_activities[-1]['start_date'][:10], '%Y-%m-%d')
    
    if duration_days > 0:
        start_date = end_date - timedelta(days=duration_days-1) #-1 because you can also do an activity on the PB day before the PB
    else:

        start_date = datetime.strptime(block_activities[0]['start_date'][:10], '%Y-%m-%d')
        duration_days = (end_date - start_date).days
        
    weeks = []
    
    for x in range(int(duration_days/7)):
        week = []
        weeks.append(week)
    
    for activity in block_activities[0:-1]: #Ignoring the last activity, which is the PB
                
        activity_date = datetime.strptime(activity['start_date'][:10], '%Y-%m-%d')
        current_week = math.floor((activity_date - start_date).days / 7)
        if current_week < len(weeks):
            weeks[current_week].append(activity)   
        
    weeks = list(tuple(dropwhile(lambda x: len(x) == 0,(weeks)))) #removes leading empty weeks
    
    return weeks

