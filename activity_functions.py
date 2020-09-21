# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 07:49:26 2019
@author: rian-van-den-ander
"""

def get_non_run_activity_data(activity, zones):
    
    import numpy
    
    activity_types = {1: 'Ride', 2: 'Run', 3: 'Swim', 4: 'Walk'
                      , 5: 'Hike', 6: 'Alpine Ski'
                      , 7: 'Backcountry Ski'
                      , 8: 'Canoe', 9: 'Crossfit', 10: 'E-BikeRide'
                      , 11: 'Elliptical', 12: 'Handcycle', 13: 'IceSkate'
                      , 14: 'InlineSkate', 15: 'Kayak'
                      , 16: 'KitesurfSession', 17: 'Nordic Ski'
                      , 18: 'RockClimb', 19: 'RollerSki'
                      , 20: 'Row', 21: 'Snowboard', 23: 'Snowshoe'
                      , 24: 'StairStepper', 25: 'StandUpPaddle'
                      , 26: 'Surf', 27: 'VirtualRide'
                      , 28: 'VirtualRun', 29: 'WeightTraining'
                      , 30: 'WindsurfSession', 31: 'Wheelchair'
                      , 32: 'Workout', 33: 'Yoga', 34: 'Other' }
    
    activity_types = {v: k for k, v in activity_types.items()}
    
    r_activity_type = 34
    
    r_activity_id = activity['id']
    
    if(activity['type'] in activity_types):
        r_activity_type = activity_types[activity['type']]
    
    r_mean_hr = None 
    try:
        
        if (not numpy.isnan(activity['average_heartrate'] )):
            r_mean_hr = activity['average_heartrate']        
            
    except Exception:
        #It is expected many activities have no heart-rate
        pass
 
    r_elapsed_time = None
    try:
        r_elapsed_time = activity['elapsed_time']    
    except Exception:
        print('failed to capture duration for non-run ' + str(activity['id']) )

    r_distance = None
    try:
        r_distance = activity['distance']    
    except Exception:
        #It is expected many activities have no distance
        pass       
  

    
    """
    Activity data I'm not using (for now):
        sets, if its a workout
        distance in laps, if it has
        more distance metrics (FFT, stdev)
    """
    return r_activity_id, r_activity_type, r_elapsed_time, r_distance, r_mean_hr

def get_run_activity_data(activity, zones, hr_regressor):
    
    r_activity_id, r_activity_type, r_elapsed_time, r_distance, r_mean_hr = get_non_run_activity_data(activity, zones)

    r_cadence = None
    try:
        r_cadence = activity['average_cadence']
    except Exception:
        #It is expected a few activities have no cadence
        pass           
    
    r_elevation = None
    try:
        r_elevation = round(float(activity['elev_high']) - float(activity['elev_low']),0)
    except Exception:
        #It is expected a few activities have no elevation
        pass           
    
    r_stdev_elevation = None
    r_freq_elevation = None

    r_pace = None
    try:
        r_pace = activity['average_speed']    
    except Exception:
        #It is expected a few activities have no average speed
        pass           
    
    r_stdev_pace = None
    r_freq_pace = None
    r_stdev_hr = None
    r_freq_hr = None
    time_in_zones = [0] * 5
    
    from statistics import stdev 
    from scipy import signal

    try:

        laps = activity['laps']        
        elevation = []
        pace = []
        cadence = []
        hr_estimate = []        
                
        if len(laps) > 0:
            
            for lap in laps:
                elevation.append(lap['total_elevation_gain'])
                pace.append(lap['average_speed'])
                
                try:
                    cadence.append(lap['average_cadence'])        
                except Exception:
                    pass
                                        
                """
                Estimate HR from pace if no HR data
                """
                hr = None
                try:
                    hr = lap['average_heartrate']
                except Exception:
                    pass
                    
                if hr is None:
                    hr = hr_regressor.predict([[lap['average_speed']]])[0][0]
                    hr_estimate.append(hr)    
                else:
                    hr_estimate.append(hr)
                
                if int(hr) < zones[0]:
                    time_in_zones[0] += 1
                elif int(hr) >= zones[0] and int(hr) < zones[1]:
                    time_in_zones[1] += 1
                elif int(hr) >= zones[1] and int(hr) < zones[2]:
                    time_in_zones[2] += 1
                elif int(hr) >= zones[2] and int(hr) < zones[3]:
                    time_in_zones[3] += 1
                elif int(hr) >= zones[3]:
                    time_in_zones[4] += 1
     
            r_mean_hr = hr_regressor.predict([[activity['average_speed']]])[0][0]
            r_stdev_hr = stdev(hr_estimate)
            peaks = signal.find_peaks(hr_estimate)
        
            if (len(peaks[0])) > 0:
                r_freq_hr =  round(len(peaks[0])/len(hr_estimate),2)
            
            for zone in range(len(time_in_zones)):
                time_in_zones[zone] = round(float(time_in_zones[zone]) / float(len(laps)),2)                
        
        try:
            r_stdev_elevation = stdev(elevation)
        except Exception:
            #It is expected a few activities have no good variance in elevation
            pass      
    
        try:
            r_stdev_pace = stdev(pace)
        except Exception:
            #It is expected a few activities have no good variance in pace
            pass      
       
        peaks = signal.find_peaks(elevation)
        
        if (len(peaks[0])) > 0:            
            r_freq_elevation =  round(len(peaks[0])/len(elevation),2)
            
        peaks = signal.find_peaks(pace)
        
        if (len(peaks[0])) > 0:            
            r_freq_pace =  round(len(peaks[0])/len(pace),2)

    except Exception as ex:
        print (str(ex))
        pass 

    
    r_athlete_count = 1
    try:
        r_athlete_count = activity['athlete_count']    
    except Exception:
        pass           
      
    return  r_activity_id, r_activity_type, r_elapsed_time, r_distance, r_mean_hr, r_stdev_hr, r_freq_hr, r_elevation, r_stdev_elevation, r_freq_elevation, r_pace, r_stdev_pace, r_freq_pace, r_cadence, r_athlete_count, time_in_zones


def get_run_hr_pace(activity, zones):
    
    r_activity_id, r_activity_type, r_elapsed_time, r_distance, r_mean_hr = get_non_run_activity_data(activity, zones)
  
    r_pace = None
    try:
        r_pace = activity['average_speed']    
    except Exception:
        #It is expected a few activities have no average speed
        pass               
      
    return r_mean_hr, r_pace



