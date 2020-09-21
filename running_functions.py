# -*- coding: utf-8 -*-
"""
Created on Sun Dec  8 08:13:53 2019
@author: rian-van-den-ander
"""

def get_pbs(activities):
    
    import math
    import datetime
    
    #PRs for a minim
    _minimum_acceptable_distance = 5000
    _maximum_acceptable_distance = 45000
    
    #threshold for 'good enough' achievement
    _minimum_vdot_increase = 0.2
    _minimum_days_apart_for_pb = 30
    
    #hacky variable initialisations
    current_marathon_best = 10*60*60
    current_vdot_best = 0
    last_pb = datetime.datetime(1900, 1, 1)
    
    significant_pbs = []
    
    for activity in activities:

        #catch invalid activities
        try:
            activity['type']    
        except Exception:
            continue
            
        
        if activity['type'] == 'Run' or activity['type'] == 'Trail Run':
                           
            if 'best_efforts' in activity:            
                for best_effort in activity['best_efforts']:
                    if int(best_effort['distance']) >= _minimum_acceptable_distance and int(best_effort['distance']) <= _maximum_acceptable_distance:   
                        
                        """ 
                        vdot calculation, taken from script here http://www.tomfangrow.com/jsvdot.html
                        and forum here https://www.letsrun.com/forum/flat_read.php?thread=4858970
                        
                        if I want to come back to suggest training zones, they are calculable here: https://www.letsrun.com/forum/flat_read.php?thread=4858970
                                            
                        data on best_effort:
                            /activity/id
                            /elapsed_time
                            /distance
                            /start_date
                            
                        output:
                            vdot = your vdot for this activity
                            marathon_pred_secs = your predicted marathon time
                        """
                                            
                        d = float(best_effort['distance'])
                        t = float(best_effort['elapsed_time']) / 60 #in MINUTES
                      
                        c=-4.6 + 0.182258 * (d/t) + 0.000104 * (d/t)**2	 	
                        i=0.8 + 0.1894393 * math.exp(-0.012778 * t)+	0.2989558 * math.exp(-0.1932605 * t)
                        vdot = (c/i)
        
                        d=42200
                        t=d*.004
                        n=0
                        e = 1.0
                        while n < 50 and e > 0.1:
                            c=-4.6 + 0.182258 * (d/t) + 0.000104 * (d/t)**2	 
                            i=0.8 + 0.1894393 * math.exp(-0.012778 * t)+	0.2989558 * math.exp(-0.1932605 * t)
                            e=abs(c-i*vdot)
                            dc=- 0.182258*d/t/t-2*.000104*d*d/t/t/t
                            di=-.012778*.1894393*math.exp(-.012778*t)- .1932605*.2989558*math.exp(-.1932605*t);
                            dt=(c-i*vdot)/(dc-di*vdot)
                            t-=dt
                            n+=1
                        
                        marathon_pred_secs = t*60
                        
                        """ 
                        Pick 'good enough' PBs: 
                            I have arbitrarily chosen a threshold as a vdot increase of _minimum_vdot_increase
                            PBs must be at least 1 month apart, also arbitrarily chosen in _minimum_days_apart_for_pb   
                        
                        """
                        
                        if(marathon_pred_secs < current_marathon_best and (vdot-current_vdot_best > _minimum_vdot_increase)):
                            current_vdot_best = vdot
                            current_marathon_best = marathon_pred_secs
                            
                            marathon_pred_hours = marathon_pred_secs / (60**2)
                            
                            pb_date = datetime.datetime.strptime(best_effort['start_date'][:10], '%Y-%m-%d')
                            time_since_last_pb = (pb_date-last_pb).days
        
                            if(time_since_last_pb > _minimum_days_apart_for_pb):
                                last_pb = pb_date
                                print ('marathon pred (hours): ' + str(marathon_pred_hours) + ', vdot: ' + str(vdot) + ', date: ' + str(pb_date)) 
                                
                                activity_id = best_effort['activity']['id']
                                
                                pb_data = []
                                pb_data.append(vdot)
                                pb_data.append(marathon_pred_hours)
                                pb_data.append(pb_date)
                                pb_data.append(activity_id)
                                significant_pbs.append(pb_data)
                                  
    
    return significant_pbs

    
def get_run_outliers(all_activities, block_id, athlete_id):

    import numpy as np
    from scipy import stats
    import pandas as pd
    
    runs = all_activities.loc[all_activities['activity_type'] == 2]
    
    block_activities = runs.loc[runs['block_id'] == block_id]
    athlete_activities = runs.loc[runs['athlete_id'] == athlete_id]
    
    distance_activities = pd.DataFrame()
    intense_activities = pd.DataFrame()
    varying_activities = pd.DataFrame()
    
    """
    Distance outliers - based on all of your activities
    """
    
    try:     
        
        distance_activities = athlete_activities.loc[(np.abs(stats.zscore(athlete_activities["distance"])) >= 1.2)] #1.2 through trial and error
        athlete_mean_distance =  float(athlete_activities['distance'].mean())
        distance_activities = distance_activities.loc[(distance_activities["distance"] >= athlete_mean_distance)] #filter out small outliers
        distance_activities = distance_activities.loc[distance_activities['block_id'] == block_id]
    except Exception:
        pass    
    
    """
    Intensity outliers - tempo runs based on all of your activities
    """
    
    try:        
    
        intense_activities_with_hr = pd.DataFrame()
        intense_activities_with_pace = pd.DataFrame()
    
        if (athlete_activities['mean_hr'].sum() != 0):
    
            activities_with_hr = athlete_activities.loc[athlete_activities['mean_hr'].isnull() == False]
            intense_activities_with_hr = activities_with_hr.loc[(np.abs(stats.zscore(activities_with_hr["mean_hr"])) >= 1.2)]
            intense_activities_with_hr = intense_activities_with_hr.loc[(intense_activities_with_hr["distance"] >= athlete_mean_distance)] #filter out small outliers
            athlete_mean_hr =  float(activities_with_hr['mean_hr'].mean())
            intense_activities_with_hr = intense_activities_with_hr.loc[(intense_activities_with_hr["mean_hr"] >= athlete_mean_hr)] #filter out small outliers
            intense_activities_with_hr = intense_activities_with_hr.loc[intense_activities_with_hr['block_id'] == block_id]
    
        if (athlete_activities['pace'].sum() != 0):
    
            activities_with_pace = athlete_activities.loc[athlete_activities['pace'].isnull() == False]
            intense_activities_with_pace = activities_with_pace.loc[(np.abs(stats.zscore(activities_with_pace["pace"])) >= 1.5)]
            intense_activities_with_pace = intense_activities_with_pace.loc[intense_activities_with_pace['block_id'] == block_id]
            intense_activities_with_hr = intense_activities_with_hr.loc[(intense_activities_with_hr["distance"] >= 1000)] #filter out small outliers
            athlete_mean_pace =  float(activities_with_pace['pace'].mean())
            intense_activities_with_pace = intense_activities_with_pace.loc[(intense_activities_with_pace["pace"] >= athlete_mean_pace)]
        
        intense_activities = pd.concat([intense_activities_with_hr,intense_activities_with_pace]).drop_duplicates().reset_index(drop=True)

    except Exception:
        pass
    
    """
    Interval outliers - runs with a lot of variance
    """

    try:     
        
        activities_with_hr = athlete_activities.loc[athlete_activities['stdev_hr'].isnull() == False]
        stdev_hr_threshold = activities_with_hr['stdev_hr'].mean()
        varying_activities_hr = block_activities.loc[block_activities['freq_hr'].isnull() == False]
        
        varying_activities_pace = block_activities.loc[block_activities['freq_pace'].isnull() == False]
    
        varying_activities = pd.concat([varying_activities_hr,varying_activities_pace]).drop_duplicates().reset_index(drop=True)
              
        varying_activities = varying_activities.loc[(varying_activities["distance"] >= 1000)] #filter out small outliers 
        varying_activities = varying_activities.loc[(varying_activities["stdev_hr"] >= stdev_hr_threshold)] 
        
    except Exception:
        pass

    return distance_activities, intense_activities, varying_activities
    
def extract_activity_features(activities, activity, zones, activity_type, athlete_id, block_id, week_id, hr_regressor):
    
    from activity_functions import get_non_run_activity_data
    from activity_functions import get_run_activity_data
    
    if(activity_type != 'Run' and activity_type != 'TrailRun'):
    
        """
        NON-RUNS
        """                            
        
        r_activity_id, r_activity_type, r_elapsed_time, r_distance, r_mean_hr = get_non_run_activity_data(activity, zones)
        activities = activities.append({'athlete_id': athlete_id,
                                            'block_id': block_id,
                                            'week_id': week_id, 
                                            'activity_type': r_activity_type,
                                            'activity_id': r_activity_id, 
                                            'elapsed_time': r_elapsed_time, 
                                            'distance': r_distance, 
                                            'mean_hr': r_mean_hr},ignore_index=True)
     
    else:
    
        """
        RUNS / TRAIL RUNS
        """ 
        
        r_activity_id, r_activity_type, r_elapsed_time, r_distance, r_mean_hr, r_stdev_hr, r_freq_hr, r_elevation, r_stdev_elevation, r_freq_elevation, r_pace, r_stdev_pace, r_freq_pace, r_cadence, r_athlete_count, time_in_zones = get_run_activity_data(activity, zones, hr_regressor)
          
        
        activities = activities.append({'athlete_id': athlete_id,
                                    'block_id': block_id,
                                    'week_id': week_id, 
                                    'activity_type': r_activity_type,
                                    'activity_id': r_activity_id, 
                                    'elapsed_time': r_elapsed_time, 
                                    'distance': r_distance, 
                                    'mean_hr': r_mean_hr, 
                                    'stdev_hr': r_stdev_hr, 
                                    'freq_hr': r_freq_hr, 
                                    'time_in_z1': time_in_zones[0], 
                                    'time_in_z2': time_in_zones[1], 
                                    'time_in_z3': time_in_zones[2], 
                                    'time_in_z4': time_in_zones[3], 
                                    'time_in_z5': time_in_zones[4],
                                    'elevation': r_elevation,
                                    'stdev_elevation': r_stdev_elevation,
                                    'freq_elevation': r_freq_elevation,
                                    'pace': r_pace,
                                    'stdev_pace': r_stdev_pace,
                                    'freq_pace': r_freq_pace,
                                    'cadence' : r_cadence,
                                    'athlete_count': r_athlete_count
                                    },ignore_index=True)        
        
    return activities
    
def extract_week_features(week_runs, week_non_runs, athlete_id, block_id, week_id, f_total_runs):
    
    f_total_run_distance = week_runs["distance"].sum()
    f_avg_run_distance = week_runs["distance"].mean()
    f_stdev_run_distance = week_runs["distance"].std()
    
    f_total_run_time = week_runs["elapsed_time"].sum()
    f_avg_run_time = week_runs["elapsed_time"].mean()
    f_stdev_run_time = week_runs["elapsed_time"].std()

    f_total_non_run_distance = week_non_runs["distance"].sum()
    f_avg_non_run_distance = week_non_runs["distance"].mean()
    f_stdev_non_run_distance = week_non_runs["distance"].std()

    f_total_non_run_time = week_non_runs["elapsed_time"].sum()
    f_avg_non_run_time = week_non_runs["elapsed_time"].mean()
    f_stdev_non_run_time = week_non_runs["elapsed_time"].std()
    
    f_time_in_z1_non_runs = week_non_runs["time_in_z1"].mean()
    f_time_in_z2_non_runs = week_non_runs["time_in_z2"].mean()
    f_time_in_z3_non_runs = week_non_runs["time_in_z3"].mean()
    f_time_in_z4_non_runs = week_non_runs["time_in_z4"].mean()
    f_time_in_z5_non_runs = week_non_runs["time_in_z5"].mean()
    
    """
    Advanced feature data: count of each non run
    """

    activity_counts = {} 
    for non_run_index, non_run in week_non_runs.iterrows():                            
    
        activity_type = 'f_activity_type_' +  str(int(non_run['activity_type']))
        
        if activity_type in activity_counts:    
            activity_counts[activity_type] += 1               
        else:
            activity_counts[activity_type] = 1
        
        pass
          
    #TODO made note ASD345 in tech debt - not sure if the below is the best way to pick up means and stdevs. could i make clusters of 
    #activity types?
    
    f_mean_run_hr = week_runs["mean_hr"].mean()
    f_mean_non_run_hr = week_non_runs["mean_hr"].mean()
    
    f_avg_stdev_run_hr = week_runs["stdev_hr"].mean()
    f_avg_stdev_non_run_hr = week_non_runs["stdev_hr"].mean()

    f_avg_freq_run_hr = week_runs["freq_hr"].mean()
    f_avg_freq_non_run_hr = week_non_runs["freq_hr"].mean()
    
    f_time_in_z1_runs = week_runs["time_in_z1"].mean()
    f_time_in_z2_runs = week_runs["time_in_z2"].mean()
    f_time_in_z3_runs = week_runs["time_in_z3"].mean()
    f_time_in_z4_runs = week_runs["time_in_z4"].mean()
    f_time_in_z5_runs = week_runs["time_in_z5"].mean()            
    
    f_elevation = week_runs["elevation"].sum()
    f_mean_elevation = week_runs["elevation"].mean()
    f_avg_stdev_elevation = week_runs["stdev_elevation"].mean()     
    f_avg_freq_elevation = week_runs["freq_elevation"].mean()         
    
    f_mean_pace = week_runs["pace"].mean()
    f_avg_stdev_pace = week_runs["stdev_pace"].mean()     
    f_avg_freq_pace = week_runs["freq_pace"].mean()    
   
    f_mean_cadence = week_runs["cadence"].mean()
    
    f_mean_athlete_count = week_runs["athlete_count"].mean()

    to_append = {'athlete_id': athlete_id,
                                    'block_id': block_id,
                                    'week_id': week_id,
                                    'f_total_runs': f_total_runs,                                          
                                    'f_total_run_distance': f_total_run_distance,
                                    'f_avg_run_distance': f_avg_run_distance,
                                    'f_stdev_run_distance': f_stdev_run_distance,
                                    'f_total_run_time': f_total_run_time,
                                    'f_avg_run_time': f_avg_run_time,
                                    'f_stdev_run_time': f_stdev_run_time,   
                                    'f_mean_run_hr': f_mean_run_hr,   
                                    'f_avg_stdev_run_hr': f_avg_stdev_run_hr,   
                                    'f_avg_freq_run_hr': f_avg_freq_run_hr,
                                    'f_time_in_z1_runs': f_time_in_z1_runs,
                                    'f_time_in_z2_runs': f_time_in_z2_runs,
                                    'f_time_in_z3_runs': f_time_in_z3_runs,
                                    'f_time_in_z4_runs': f_time_in_z4_runs,
                                    'f_time_in_z5_runs': f_time_in_z5_runs,
                                    
                                    'f_elevation': f_elevation,                                          
                                    'f_mean_elevation': f_mean_elevation,
                                    'f_avg_stdev_elevation': f_avg_stdev_elevation,
                                    'f_avg_freq_elevation': f_avg_freq_elevation,
                                    'f_mean_pace': f_mean_pace,
                                    'f_avg_stdev_pace': f_avg_stdev_pace,
                                    'f_avg_freq_pace': f_avg_freq_pace,   
                                    'f_mean_cadence': f_mean_cadence,
                                    'f_mean_athlete_count': f_mean_athlete_count,    
                                      
                                    'f_total_non_run_distance': f_total_non_run_distance,
                                    'f_avg_non_run_distance': f_avg_non_run_distance,
                                    'f_stdev_non_run_distance': f_stdev_non_run_distance,
                                    'f_total_non_run_time': f_total_non_run_time,
                                    'f_avg_non_run_time': f_avg_non_run_time,
                                    'f_stdev_non_run_time': f_stdev_non_run_time,    
                                    'f_mean_non_run_hr': f_mean_non_run_hr,    
                                    'f_avg_stdev_non_run_hr': f_avg_stdev_non_run_hr,     
                                    'f_avg_freq_non_run_hr': f_avg_freq_non_run_hr,
                                    'f_time_in_z1_non_runs': f_time_in_z1_non_runs,
                                    'f_time_in_z2_non_runs': f_time_in_z2_non_runs,
                                    'f_time_in_z3_non_runs': f_time_in_z3_non_runs,
                                    'f_time_in_z4_non_runs': f_time_in_z4_non_runs,
                                    'f_time_in_z5_non_runs': f_time_in_z5_non_runs,                                           
                                    }
    
    to_append.update(activity_counts)            
    
    return to_append       

def build_pace_to_hr_regressor(activities, athlete_id, zones):
    
    from activity_functions import get_run_hr_pace
    import pandas as pd
    import numpy as np
    paces_and_hrs = pd.DataFrame()    

    for activity in activities:
        
        try:
            catchme = activity['type']
        except Exception:
            continue
        
        if(activity['type'] == 'Run'):            
            
            r_mean_hr, r_pace = get_run_hr_pace(activity, zones)
                          
            paces_and_hrs = paces_and_hrs.append({'athlete_id': athlete_id,
                                        'mean_hr': r_mean_hr, 
                                        'pace': r_pace
                                        },ignore_index=True)        
    
        
    if(len(paces_and_hrs) == 0):
        return None, None
        
    from sklearn.linear_model import LinearRegression
    import math
            
    not_nan_rows = paces_and_hrs[~paces_and_hrs['mean_hr'].isnull()]
    nan_rows = paces_and_hrs[paces_and_hrs['mean_hr'].isnull()]
    nan_rows = nan_rows[~nan_rows['pace'].isnull()]
    
    regressor = None
    
    #build a regressor for pace -> heart rate
    if(len(not_nan_rows) > 5):
        #populate from this athlete's pace->hr mapping. build a little regression model for it
        athlete_hrs = list(not_nan_rows['mean_hr'])    
        athlete_hrs = np.array(athlete_hrs)
        list_paces = list(not_nan_rows['pace'])                
        average_pace = np.nanmean(list_paces)
        list_paces = np.array([average_pace if math.isnan(x) else x for x in list_paces])
        athlete_hrs = athlete_hrs.reshape(-1, 1)
        list_paces = list_paces.reshape(-1, 1)
        lin_reg = LinearRegression()
        regressor = lin_reg.fit(list_paces, athlete_hrs)        
                   
    return regressor, not_nan_rows