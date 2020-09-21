# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 07:49:12 2019

@author: rian-van-den-ander
"""

from sql_methods import read_db
import datetime
import pandas as pd
import io
import math
import matplotlib.pyplot as plt
import matplotlib    


feature_labels = {'f_slope_distances_before_taper': 'Week on week distance increase\n(metres)',
                    'f_taper_factor_distance': 'Distance tapering\n(ratio decreased for last two weeks)',
                    'f_slope_time_before_taper': 'Week on week running time increase\n(seconds)',
                    'f_taper_factor_time': 'Decrease in time when tapering\n(ratio)',
                    'f_slope_hr_before_taper': 'Week on week intensity increase\n(average HR)',
                    'f_taper_factor_hr': 'Heart rate (intensity) tapering\n(ratio)',
                    'f_avg_weekly_run_distance': 'Weekly run distance\n(metres)',
                    'r_avg_weekly_run_distance': 'RELATIVE: Average weekly run distance\n(ratio: this period vs all your data)',
                    #'f_avg_weekly_non_run_distance': f_avg_weekly_non_run_distance,
                    #'r_avg_weekly_non_run_distance': r_avg_weekly_non_run_distance,
                    #'f_avg_weekly_run_time': f_avg_weekly_run_time,
                    #'r_avg_weekly_run_time': r_avg_weekly_run_time,
                    'f_avg_weekly_non_run_time': 'Weekly time spent on other activities\n(seconds)',
                    'r_avg_weekly_non_run_time': 'RELATIVE: Weekly time spent on other activities\n(ratio: this period vs all your data)',
                    'f_avg_total_runs': 'Weekly run count\n(number of runs)',
                    'r_avg_total_runs': 'RELATIVE: Weekly run count\n(ratio: this period vs all your data)',
                    'f_avg_weekly_run_elevation': 'Weekly run elevation\n(metres)',
                    'r_avg_weekly_run_elevation': 'RELATIVE: Weekly run elevation\n(ratio: this period vs all your data)',
                    'f_mean_athlete_count': 'How many athletes you trained with on average\n(people)',
                    'r_mean_athlete_count': 'RELATIVE: Athletes trained with\n(ratio: this period vs all your data)',
                    'f_avg_time_in_z1_runs': 'Weekly time in Z1 for runs\n(out of 1)',
                    'f_avg_time_in_z2_runs': 'Weekly time in Z2 for runs\n(out of 1)',
                    'f_avg_time_in_z3_runs': 'Weekly time in Z3 for runs\n(out of 1)',
                    'f_avg_time_in_z4_runs': 'Weekly time in Z4 for runs\n(out of 1)',
                    'f_avg_time_in_z5_runs': 'Weekly time in Z5 for runs\n(out of 1)',
                    'r_avg_time_in_z1_runs': 'RELATIVE: Weekly time in Z1 for runs\n(ratio: this period vs all your data)',
                    'r_avg_time_in_z2_runs': 'RELATIVE: Weekly time in Z2 for runs\n(ratio: this period vs all your data)',
                    'r_avg_time_in_z3_runs': 'RELATIVE: Weekly time in Z3 for runs\n(ratio: this period vs all your data)',
                    'r_avg_time_in_z4_runs': 'RELATIVE: Weekly time in Z4 for runs\n(ratio: this period vs all your data)',
                    'r_avg_time_in_z5_runs': 'RELATIVE: Weekly time in Z5 for runs\n(ratio: this period vs all your data)',
                    'f_proportion_distance_activities': 'Proportion of long runs\n(ratio)',
                    'f_proportion_intense_activities': 'Proportion of intense runs\n(ratio)',
                    'f_proportion_varying_activities': 'Proportion of interval runs\n(ratio)',  
                    'r_proportion_distance_activities': 'RELATIVE: Proportion of long runs\n(ratio: this period vs all your data)',
                    'r_proportion_intense_activities': 'RELATIVE: Proportion of intense runs\n(ratio: this period vs all your data)',
                    'r_proportion_varying_activities': 'RELATIVE: Proportion of interval runs\n(ratio: this period vs all your data)',  
                    'f_proportion_rides': 'Proportion of rides\n(ratio)',  
                    'f_proportion_swims': 'Proportion of swims\n(ratio)',  
                    'f_proportion_walks_hikes': 'Proportion of walks or hikes\n(ratio)',  
                    'f_proportion_alpine_ski': 'Proportion of alpine skiing\n(ratio)',  
                    'f_proportion_workout': 'Proportion of workouts\n(ratio)',  
                    'f_proportion_yoga': 'Proportion of yoga sessions\n(ratio)',  
                    'f_proportion_crossfit': 'Proportion of crossfit sessions\n(ratio)',   
                    'f_proportion_other': 'Proportion of other activities\n(ratio)',               
                    'r_proportion_rides': 'Proportion of rides\n(ratio)',
                    'r_proportion_swims': 'Proportion of swims\n(ratio)', 
                    'r_proportion_walks_hikes': 'Proportion of walks or hikes\n(ratio)', 
                    'r_proportion_alpine_ski': 'Proportion of alpine skiing\n(ratio)', 
                    'r_proportion_workout': 'Proportion of workouts\n(ratio)', 
                    'r_proportion_yoga': 'Proportion of yoga sessions\n(ratio)', 
                    'r_proportion_crossfit': 'Proportion of crossfit sessions\n(ratio)', 
                    'r_proportion_other': 'Proportion of other activities\n(ratio)',                                   
                    } 

def athletevsbest(athlete_id):

    """
    read initial data, initialise stuff
    """
    athlete_id = int(athlete_id)
    features_blocks = read_db('features_blocks') 
    metadata_blocks = read_db('metadata_blocks') 
    model_outputs = read_db('model_outputs')
        
    """
    grab last block by this athlete
    """
    this_athlete_blocks = features_blocks[features_blocks['athlete_id']==athlete_id]
    this_athlete_last_block = this_athlete_blocks.iloc[-1]    
    
    """
    grab stats on this block - start date, end date, vdot, marathon prediction time
    """
    end_date = metadata_blocks[metadata_blocks['block_id'].astype(float) == float(this_athlete_last_block['block_id'])].iloc[0]['pb_date']
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    start_date = end_date - datetime.timedelta(days = 91)
    
    # output date: print('Date:', end_date.date())
    
    """
    get ordered list of nn features from db
    Here, we are looking at y_vdot as the model trainer
    """    
    features = model_outputs[model_outputs['y_name']=='y_vdot']
    features = features.sort_values(['importance'], ascending=[0])
    top_ten_percent = features_blocks.sort_values(['y_vdot'], ascending=[0]).head(round(0.1*len(features_blocks)))
    bottom_ten_percent = features_blocks.sort_values(['y_vdot'], ascending=[1]).head(round(0.1*len(features_blocks)))
       
    visualisation_outputs = pd.DataFrame()
    
    for index, feature in features.head(20).iterrows():
        
        feature_name = feature['feature_name']
        feature_importance = feature['importance']        
        
        """
        athlete's score for this feature
        and percentile
        """
        
        athlete_score = round(this_athlete_last_block[feature_name],2)
        top_ten_percent_value = top_ten_percent[feature_name].mean() 
        bottom_ten_percent_value = bottom_ten_percent[feature_name].mean()        
 
        # skipping broken features
        if(feature_name == "f_proportion_other" or feature_name == "r_proportion_other"):
            continue    
    
        # last minute data cleaning
        if(top_ten_percent_value == 0.0 and bottom_ten_percent_value == 0.0):
            continue
        if(math.isnan(athlete_score)):
            athlete_score = 0.0
        if(math.isnan(top_ten_percent_value) or math.isnan(bottom_ten_percent_value)):
            continue

        perc_compare_top = top_ten_percent_value
        perc_compare_bottom = bottom_ten_percent_value
       
        if(bottom_ten_percent_value > top_ten_percent_value):
            print(feature_name + ' SWAP')
            switch = perc_compare_top
            perc_compare_top = perc_compare_bottom
            perc_compare_bottom = switch                
            
            if(athlete_score > perc_compare_bottom):
                athlete_percentile = 5
            elif(athlete_score < perc_compare_top):
                athlete_percentile = 95
            else:
                athlete_percentile = 100*((((athlete_score - bottom_ten_percent_value) / (top_ten_percent_value - bottom_ten_percent_value))*0.8) + 0.1)
                                
        else:
                
            
            if(athlete_score < perc_compare_bottom):
                athlete_percentile = 5
            elif(athlete_score > perc_compare_top):
                athlete_percentile = 95
            else:
                athlete_percentile = 100*((((athlete_score - bottom_ten_percent_value) / (top_ten_percent_value - bottom_ten_percent_value))*0.8) + 0.1)

        athlete_need = feature_importance * (100-athlete_percentile)
                    
        visualisation_outputs = visualisation_outputs.append({'feature_name':feature_name,
                                      'feature_importance':feature_importance,
                                      'athlete_score':athlete_score,
                                      'athlete_percentile':athlete_percentile,
                                      'athlete_need':athlete_need,
                                      'tenth':10,
                                      'ninetieth':90,
                                      'one-hundredth':100,
                                      'value_at_tenth':round(bottom_ten_percent_value,2),
                                      'value_at_ninetieth':round(top_ten_percent_value,2)
                                      },ignore_index=True)        

    visualisation_outputs = visualisation_outputs.sort_values(by=['athlete_need'],ascending=False)
    visualisation_outputs = visualisation_outputs.iloc[::-1]
    visualisation_outputs= visualisation_outputs.reset_index()
    
    plt.style.use(u'seaborn-darkgrid')
    plt.title("Your performance relative to the best athletes \n For 3 months before your last PB, between "
              + str(start_date.date()) + " and " + str(end_date.date())
              + "\nOrdered by how much each aspect would help your fitness")

    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(12, 12.5)
    
    labels = []
    
    for index, feature in visualisation_outputs.iterrows():
        labels.append(feature_labels[feature['feature_name']] + "\n" + str(feature['athlete_score']))
        
    ax = fig.add_subplot(111)
    
    ax.barh(labels,  visualisation_outputs['one-hundredth'], tick_label=labels, height=0.8, color='#afffd3')
    ax.barh(labels,  visualisation_outputs['ninetieth'], tick_label=labels, height=0.8, color='#bbbbc1')
    ax.barh(labels,  visualisation_outputs['tenth'], tick_label=labels, height=0.8, color='#ffa4a4')    


    ax.plot(visualisation_outputs['athlete_percentile'], labels, marker=10, markersize=15, linestyle="", label=visualisation_outputs['athlete_score'])

    for index, feature in visualisation_outputs.iterrows():
        ax.text(x=float(11), y=index, s=feature['value_at_tenth'], horizontalalignment="left")
        ax.text(x=float(89), y=index, s=feature['value_at_ninetieth'], horizontalalignment="right")
        
    plt.xlabel('Percentile. 0% = the worst performing athlete. 100% = the best performing athlete.')    
    plt.tight_layout()
    
    bytes_image = io.BytesIO()
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    plt.clf()
    plt.cla()
    plt.close()
    return bytes_image




def athletevsbestimprovement(athlete_id):

    """
    read initial data, initialise stuff
    """
    athlete_id = int(athlete_id)
    features_blocks = read_db('features_blocks') 
    metadata_blocks = read_db('metadata_blocks') 
    model_outputs = read_db('model_outputs')
        
    """
    grab last block by this athlete
    """
    this_athlete_blocks = features_blocks[features_blocks['athlete_id']==athlete_id]
    this_athlete_last_block = this_athlete_blocks.iloc[-1]    
    
    """
    grab stats on this block - start date, end date, vdot, marathon prediction time
    """
    end_date = metadata_blocks[metadata_blocks['block_id'].astype(float) == float(this_athlete_last_block['block_id'])].iloc[0]['pb_date']
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    start_date = end_date - datetime.timedelta(days = 91)
    
    # output date: print('Date:', end_date.date())
    
    """
    get ordered list of nn features from db
    Here, we are looking at y_vdot as the model trainer
    """    
    features = model_outputs[model_outputs['y_name']=='y_vdot_delta']
    features = features.sort_values(['importance'], ascending=[0])
    top_ten_percent = features_blocks.sort_values(['y_vdot_delta'], ascending=[0]).head(round(0.1*len(features_blocks)))
    bottom_ten_percent = features_blocks.sort_values(['y_vdot_delta'], ascending=[1]).head(round(0.1*len(features_blocks)))
       
    visualisation_outputs = pd.DataFrame()
    
    for index, feature in features.head(20).iterrows():
        
        feature_name = feature['feature_name']
        feature_importance = feature['importance']        
        
        """
        athlete's score for this feature
        and percentile
        """
        
        athlete_score = round(this_athlete_last_block[feature_name],2)
        top_ten_percent_value = top_ten_percent[feature_name].mean() 
        bottom_ten_percent_value = bottom_ten_percent[feature_name].mean()        
 
        
        if(feature_name == "f_proportion_other" or feature_name == "r_proportion_other"):
            continue    
    
        # last minute data cleaning
        if(top_ten_percent_value == 0.0 and bottom_ten_percent_value == 0.0):
            continue
        if(math.isnan(athlete_score)):
            athlete_score = 0.0
        if(math.isnan(top_ten_percent_value) or math.isnan(bottom_ten_percent_value)):
            continue

        perc_compare_top = top_ten_percent_value
        perc_compare_bottom = bottom_ten_percent_value
       
        if(bottom_ten_percent_value > top_ten_percent_value):
            print(feature_name + ' SWAP')
            switch = perc_compare_top
            perc_compare_top = perc_compare_bottom
            perc_compare_bottom = switch                
            
            if(athlete_score > perc_compare_bottom):
                athlete_percentile = 5
            elif(athlete_score < perc_compare_top):
                athlete_percentile = 95
            else:
                athlete_percentile = 100*((((athlete_score - bottom_ten_percent_value) / (top_ten_percent_value - bottom_ten_percent_value))*0.8) + 0.1)
                                
        else:
                
            
            if(athlete_score < perc_compare_bottom):
                athlete_percentile = 5
            elif(athlete_score > perc_compare_top):
                athlete_percentile = 95
            else:
                athlete_percentile = 100*((((athlete_score - bottom_ten_percent_value) / (top_ten_percent_value - bottom_ten_percent_value))*0.8) + 0.1)

        athlete_need = feature_importance * (100-athlete_percentile)
                    
        visualisation_outputs = visualisation_outputs.append({'feature_name':feature_name,
                                      'feature_importance':feature_importance,
                                      'athlete_score':athlete_score,
                                      'athlete_percentile':athlete_percentile,
                                      'athlete_need':athlete_need,
                                      'tenth':10,
                                      'ninetieth':90,
                                      'one-hundredth':100,
                                      'value_at_tenth':round(bottom_ten_percent_value,2),
                                      'value_at_ninetieth':round(top_ten_percent_value,2)
                                      },ignore_index=True)        

    visualisation_outputs = visualisation_outputs.sort_values(by=['athlete_need'],ascending=False)
    visualisation_outputs = visualisation_outputs.iloc[::-1]
    visualisation_outputs= visualisation_outputs.reset_index()
    
    
    plt.style.use(u'seaborn-darkgrid')
    plt.title("Your ability, relative to others, to IMPROVE your fitness \n For 3 months before your last PB, between "
              + str(start_date.date()) + " and " + str(end_date.date())
              + "\nOrdered by how much each item will help you improve")

    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(12, 12.5)
    
    labels = []
    
    for index, feature in visualisation_outputs.iterrows():
        labels.append(feature_labels[feature['feature_name']] + "\n" + str(feature['athlete_score']))
        
    ax = fig.add_subplot(111)
    
    ax.barh(labels,  visualisation_outputs['one-hundredth'], tick_label=labels, height=0.8, color='#afffd3')
    ax.barh(labels,  visualisation_outputs['ninetieth'], tick_label=labels, height=0.8, color='#bbbbc1')
    ax.barh(labels,  visualisation_outputs['tenth'], tick_label=labels, height=0.8, color='#ffa4a4')    


    ax.plot(visualisation_outputs['athlete_percentile'], labels, marker=10, markersize=15, linestyle="", label=visualisation_outputs['athlete_score'])

    for index, feature in visualisation_outputs.iterrows():
        ax.text(x=float(11), y=index, s=feature['value_at_tenth'], horizontalalignment="left")
        ax.text(x=float(89), y=index, s=feature['value_at_ninetieth'], horizontalalignment="right")
        
    plt.xlabel('Percentile. 0% = the least improvement. 100% = the best improvement.')    
    plt.tight_layout()
    
    bytes_image = io.BytesIO()
    plt.savefig(bytes_image, format='png')
    bytes_image.seek(0)
    plt.clf()
    plt.cla()
    plt.close()
    return bytes_image













def double_to_hours_minutes(time):
    
    hours = int(math.floor(time))
    minutes = int(round(60*(time - hours),0))
    return str(datetime.time(hours, minutes, 0, 0))