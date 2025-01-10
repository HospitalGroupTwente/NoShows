import pandas as pd
import numpy as np

def add_working_days(df: pd.DataFrame, column: str, exclude_days: int) -> pd.DataFrame:
    '''
    Adds a specified number of working days (excluding weekends) to a date column.
    '''

    # add the working days using numpy.busday_offset
    df[column] = df[column].apply(
        lambda date: np.busday_offset(date.date(), exclude_days, roll='forward')
    )

    # convert back to datetime
    df[column] = pd.to_datetime(df[column])
    
    return df


def get_rolling_feature(df, history_years, exclude_days, rolling_func, feature, new_feature):
    
    # create rolling windows, df_exclude will be used to remove the data about appointments of the previous n days. This is done because in deployment we will be predicting no shows over n days
    window_days = 365 * history_years
    df_window = df.reset_index().set_index('STARTDATEPLAN').groupby('PATIENTNR', sort=False)[[feature]].rolling(f'{window_days}D', )
    df_exclude = df.reset_index().set_index('STARTDATEPLAN').groupby('PATIENTNR', sort=False)[[feature]].rolling(f'{exclude_days}D')
    
    # calculate the new feature by applying the rolling_func and extracting occurenced in the last exclude_days
    if rolling_func == 'sum':
        new_rolling_feature = (df_window[feature].sum() - df_exclude[feature].sum().fillna(0)).reset_index().rename(columns={feature: new_feature})
    elif rolling_func == 'count':
        new_rolling_feature = (df_window[feature].count() - df_exclude[feature].count().fillna(0)).reset_index().rename(columns={feature: new_feature})
    else:
        raise ValueError
    
    # add the feature
    df = df.merge(new_rolling_feature, on=['STARTDATEPLAN', 'PATIENTNR'])
    
    return df

def get_feature_of_last_appointment(df, exclude_days, feature, new_feature):
    
    # calculate status of the last appointment, excluding the appointments from the last 3 days
    df = df.sort_values(by=['PATIENTNR', 'STARTDATEPLAN'])

    ## create a shifted df to use for merging
    df_shifted = df.copy()
    df_shifted = add_working_days(df_shifted, 'STARTDATEPLAN', exclude_days)
    df_shifted[new_feature] = df_shifted[feature]

    ## perform an asof merge, this is a merge where it tries to find an match to join on and otherwise finds the closes possible value to merge on
    ## we use direction='backwards' to find the appointment most close to the date of appointment date minus 3 days (offset)
    df = pd.merge_asof(df.sort_values(by=['STARTDATEPLAN']), 
                       df_shifted[['PATIENTNR', 'STARTDATEPLAN', new_feature]].sort_values(by=['STARTDATEPLAN']),
                       
                       on='STARTDATEPLAN', by='PATIENTNR', direction='backward'
                       )
    
    return df

def calculate_cum_features(df: pd.DataFrame, history_years : int=5, exclude_days=3):
    '''
    Calculates the cumalutive features for each appointment
    This is based on a history of the patient of n years ago

    The features consists out of:
    (1) number of no shows (2) number of appointments (3) percentage of no shows 
    (4) mean difference between arrival and appointment time (5) days since last appointment
    (6) appointment last week y/n
    '''

    # Rolling features can't be calculated on non-unique index
    df = df.set_index(["PATIENTNR", "STARTDATEPLAN"])
    df = df[~df.index.duplicated(keep="last")].reset_index()
    df = df.sort_values(by=['STARTDATEPLAN', 'PATIENTNR'])

    df = df.pipe(get_rolling_feature, history_years, exclude_days, 'sum', 'no_show', 'num_no_shows') \
           .pipe(get_rolling_feature, history_years, exclude_days, 'count', 'no_show', 'num_appointments') \
           .pipe(get_rolling_feature, history_years, exclude_days, 'sum', 'VerschilAankomstEnStart', 'sum_arrival_times') \
           .pipe(get_feature_of_last_appointment, exclude_days, 'no_show', 'last_noshow') \
           .pipe(get_feature_of_last_appointment, exclude_days, 'STARTDATEPLAN', 'last_appointment_date') \
    
    # calculate percentage of no shows
    df['perc_no_shows'] = df['num_no_shows'].fillna(0) / df['num_appointments'].fillna(0)
    
    # calculate mean arrival time
    df['stiptheid']     = df['sum_arrival_times'] / (df['num_appointments'].fillna(0) - df['num_no_shows'].fillna(0))
    df.loc[np.isinf(df['stiptheid']), 'stiptheid'] = df.loc[np.isinf(df['stiptheid']), 'sum_arrival_times'] / df.loc[np.isinf(df['stiptheid']), 'num_appointments']
    
    # calculate days since the last appointment
    df['days_since_last_appointment'] = (df['STARTDATEPLAN'] - df['last_appointment_date']).dt.days

    return df

