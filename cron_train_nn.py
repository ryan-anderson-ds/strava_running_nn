#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 17:43:36 2019
@author: rian-van-den-ander
"""
import numpy as np
import pandas as pd
import numpy.ma as ma
from sklearn.metrics import r2_score
from sql_methods import write_db_replace, write_db_insert, read_db
from datetime import date

def cron_train_nn():

    model_outputs = pd.DataFrame()
    
    features_blocks = read_db('features_blocks')

    df = features_blocks #just to make debugging easier with same names 

    X = df.iloc[:, 2:-2]
    X['r_proportion_alpine_ski'] = 0
    X['r_proportion_crossfit'] = 0

    X = np.where(np.isnan(X), ma.array(X, mask=np.isnan(X)).mean(axis=0), X)

    """
    PREDICT ABSOLUTE VDOT
    """

    y = df.iloc[:, -2] #-1: change in vdot. #2: absolute vdot

    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2)

    """
    XGBOOST
    
    from xgboost import XGBRegressor
    regressor = XGBRegressor()

    regressor.fit(X_train,y_train)

    # Predicting a new result
    y_pred = regressor.predict(X_test)
    y_actual = y_test

    print('xgboost single score:')
    print(r2_score(y_actual, y_pred)) 
    """

    from sklearn.ensemble import RandomForestRegressor
    regressor = RandomForestRegressor(n_estimators = 50) 
    regressor.fit(X_train,y_train)

    # Predicting a new result
    y_pred = regressor.predict(X_test)
    y_actual = y_test


    model_score = r2_score(y_actual, y_pred)
    features = list(df.columns.values[2:-2])
    importances = regressor.feature_importances_
    indices = np.argsort(importances)

    for index in indices:        
        y_name = df.columns.values[-2]
        feature_name = features[index]
        importance = importances[index]
        model_score = model_score
        model_run_date = str(date.today())
        model_outputs = model_outputs.append({'y_name': y_name,
                                                      'feature_name': feature_name,
                                                      'importance': importance,
                                                      'model_score': model_score,
                                                      'model_run_date': model_run_date}, ignore_index = True)

    """
    PREDICT CHANGE IN VDOT
    """

    y = df.iloc[:, -1] #-1: change in vdot. #2: absolute vdot

    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2)

    """
    XGBOOST
    
    from xgboost import XGBRegressor
    regressor = XGBRegressor()

    regressor.fit(X_train,y_train)

    # Predicting a new result
    y_pred = regressor.predict(X_test)
    y_actual = y_test

    print('xgboost single score:')
    print(r2_score(y_actual, y_pred)) 
    """

    from sklearn.ensemble import RandomForestRegressor
    regressor = RandomForestRegressor(n_estimators = 50) 
    regressor.fit(X_train,y_train)

    # Predicting a new result
    y_pred = regressor.predict(X_test)
    y_actual = y_test


    model_score = r2_score(y_actual, y_pred)
    features = list(df.columns.values[2:-2])
    importances = regressor.feature_importances_
    indices = np.argsort(importances)

    for index in indices:        
        y_name = df.columns.values[-1]
        feature_name = features[index]
        importance = importances[index]
        model_score = model_score
        model_run_date = str(date.today())
        model_outputs = model_outputs.append({'y_name': y_name,
                                                      'feature_name': feature_name,
                                                      'importance': importance,
                                                      'model_score': model_score,
                                                      'model_run_date': model_run_date}, ignore_index = True)

    """
    SAVE RESULTS
    """
    
    write_db_replace(model_outputs, 'model_outputs')         
    
    
    """ 
    OUTPUTS FOR MY OWN ANALYSIS
    """

    import shap
    explainer = shap.TreeExplainer(regressor)
    shap_values = explainer.shap_values(X)
    shap.summary_plot(shap_values, df.iloc[:, 2:-2])
   
    shap_values = shap.TreeExplainer(regressor).shap_values(X_train)
    shap.summary_plot(shap_values, df.iloc[:, 2:-2], plot_type="bar")
	    
    """
    Temporary measure for building model with small datasets: doing 10 train/test splits and taking average r^2
    """

    r2_total = 0
    best_r2 = -50

    for i in range(1,500):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2)
        regressor = RandomForestRegressor(n_estimators = 50) 
        regressor.fit(X_train,y_train)
        y_pred = regressor.predict(X_test)
        y_actual = y_test
        r2_total += r2_score(y_actual, y_pred)
        if(r2_score(y_actual, y_pred) > best_r2):
            best_r2 = r2_score(y_actual, y_pred)

    r2 = r2_total / (i-1)
    print('random forest on ' + str(i) + ' train/test splits:' + str(r2))
    print('best random forest on ' + str(i) + ' train/test splits:' + str(best_r2))

    return 0
