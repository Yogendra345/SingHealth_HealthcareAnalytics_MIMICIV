import pandas as pd
import numpy as np
from datetime import timedelta
import os

def count_n_days(df_master, n_days):    
    column_list = list(df_master.columns) # get list of column names in df_master to keep the updated n_icu_30/90/365d in the same position   
    df_master = df_master.drop(columns=['n_icu_'+str(n_days)+'d']) # drop original n_icu_30/90/365d column
    df_master['intime_ed'] = pd.to_datetime(df_master['intime_ed']) # convert to datetime for merging later
    
    df_icu = df_master[['subject_id', 'intime_ed', 'intime_icu']] # extract columns needed for computation of past ICU count
    df_icu.intime_ed = pd.to_datetime(df_icu.intime_ed) # convert intime to datetime type
    df_icu.intime_icu = pd.to_datetime(df_icu.intime_icu) # convert intime to datetime type    
    
    icu_df = pd.DataFrame(df_icu.dropna().groupby('subject_id')['intime_icu'].apply(set).apply(list)) # generated list of all unique ICU intime for the same patient
    icu_df = icu_df.rename(columns={'intime_icu': 'intime_icu_list'}) # rename list column
    df_icu = df_icu.merge(icu_df, left_on='subject_id', right_on='subject_id') # join icu intime list to df_icu
    
    def count_n_days(intime_ed, intime_icu_list, n_days):
        time_diff = pd.to_datetime(intime_ed) - pd.to_datetime(intime_icu_list) # compute time difference betweeen ED intime and each ICU in time for each patient
        count = sum(timedelta(days=n_days)>x>timedelta(days=0.0) for x in time_diff) # count no. of ED-ICU combinations where time difference is between 0 and the no. of days specified (exclusive)
        return count
    
    df_icu['n_icu_'+str(n_days)+'d'] = np.vectorize(count_n_days)(df_icu['intime_ed'], df_icu['intime_icu_list'], float(n_days)) # apply function to compute past ICUs, vectorize to make it faster
    
    df_icu = df_icu[['subject_id','intime_ed','n_icu_'+str(n_days)+'d']] # only keep columns for merging
    df_master = df_master.merge(df_icu, left_on=['subject_id','intime_ed'], right_on=['subject_id','intime_ed'], how='left') # merge updated past ICU count to df_master
    df_master['n_icu_'+str(n_days)+'d'] = df_master['n_icu_'+str(n_days)+'d'].fillna(0) # convert NaN to 0
    df_master['n_icu_'+str(n_days)+'d'] = df_master['n_icu_'+str(n_days)+'d'].astype(int) # convert float to integer
    
    df_master = df_master[column_list] # rearrange columns so that n_icu_30/90/365d kept within same column position                                                              
                                                              
    return df_master