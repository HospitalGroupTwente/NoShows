import pandas as pd
import numpy as np
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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


def get_rolling_feature(df: pd.DataFrame, history_years: int, exclude_days: int, rolling_func: str, feature: str, new_feature: str, groupby_col: str = None):
    '''
    Calculates a rolling feature, i.e. a feature that is obtained by applying a function on a rolling window
    It creates a rolling window for the amount of history_years specified, without the exclude_days, and applies the rolling_func on this window.

    Paramters
    ---------
    df :  pd.DataFrame
        Dataframe containing all the appointment data
    history_years : int
        amount of years of the rolling window
    exclude_days : int
        number of days that are excluded from the rolling window, i.e. 3 if you want to predict no shows for over 3 days
    rolling_func : str
        flag for which rolling function is applied on the rolling window.
        This has to be either sum or count
    feature : str
        name of the column on which the rolling_func is applied
    new_feature : str
        name of the column of your new feature, this can be whatever suits
    groupby_col : str
        column on which to do a groupby
    
    Returns
    -------
    pd.DataFrame
        Dataframe containing the new rolling feature
    '''
    groupby_cols = ['PATIENTNR']
    if groupby_col:
        groupby_cols.append(groupby_col)
    
    # create rolling windows, df_exclude will be used to remove the data about appointments of the previous n days. This is done because in deployment we will be predicting no shows over n days
    window_days = 365 * history_years
    df_window = df.reset_index().set_index('STARTDATEPLAN').groupby(groupby_cols, sort=False)[[feature]].rolling(f'{window_days}D', )
    df_exclude = df.reset_index().set_index('STARTDATEPLAN').groupby(groupby_cols, sort=False)[[feature]].rolling(f'{exclude_days}D')
    
    # calculate the new feature by applying the rolling_func and extracting occurenced in the last exclude_days
    if rolling_func == 'sum':
        new_rolling_feature = (df_window[feature].sum() - df_exclude[feature].sum().fillna(0)).reset_index().rename(columns={feature: new_feature})
    elif rolling_func == 'count':
        new_rolling_feature = (df_window[feature].count() - df_exclude[feature].count().fillna(0)).reset_index().rename(columns={feature: new_feature})
    else:
        raise ValueError
    
    # add the feature
    if groupby_col:
        df = df.merge(new_rolling_feature.drop(columns=[groupby_col]), on=['STARTDATEPLAN', 'PATIENTNR'])
    else:
        df = df.merge(new_rolling_feature, on=['STARTDATEPLAN', 'PATIENTNR'])
    
    logger.info(f'processed {new_feature}')
    
    return df

def get_feature_of_last_appointment(df, exclude_days, feature, new_feature):
    '''
    Fetches a feature of the previous appointment

    Paramters
    ---------
    df :  pd.DataFrame
        Dataframe containing all the appointment data
    exclude_days : int
        number of days that are excluded from determining what the last appointment was, i.e. 3 if you want to predict no shows for over 3 days
    feature : str
        name of the column on which is applied
    new_feature : str
        name of the column of your new feature, this can be whatever suits
    
    Returns
    -------
    pd.DataFrame
        Dataframe containing the new fast appointment feature
    '''
    
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
    logger.info(f'processed {new_feature}')
    return df

def calculate_cum_features(df: pd.DataFrame, history_years : int=5, exclude_days=3):
    '''
    Calculates the cumalutive features for each appointment
    This is based on a history of the patient of n years ago

    The features consists out of:
    (1) number of no shows 
    (2) number of appointments 
    (3) percentage of no shows 
    (4) mean difference between arrival and appointment time 
    (5) days since last appointment
    '''

    # Rolling features can't be calculated on non-unique index
    df = df.set_index(["PATIENTNR", "STARTDATEPLAN"])
    df = df[~df.index.duplicated(keep="last")].reset_index()
    df = df.sort_values(by=['STARTDATEPLAN', 'PATIENTNR'])

    df = df.pipe(get_rolling_feature, history_years, exclude_days, rolling_func='sum',   feature='no_show', new_feature='num_no_shows') \
           .pipe(get_rolling_feature, history_years, exclude_days, rolling_func='count', feature='no_show', new_feature='num_appointments') \
           .pipe(get_rolling_feature, history_years, exclude_days, rolling_func='sum',   feature='no_show', new_feature='num_no_shows_spec', groupby_col='SPECIALISME') \
           .pipe(get_rolling_feature, history_years, exclude_days, rolling_func='sum',   feature='no_show', new_feature='num_appointments_spec', groupby_col='SPECIALISME') \
           .pipe(get_rolling_feature, history_years, exclude_days, rolling_func='count', feature='no_show', new_feature='num_appointments_same_location', groupby_col='LOCATIE') \
           .pipe(get_rolling_feature, history_years, exclude_days, rolling_func='sum',   feature='VerschilAankomstEnStart', new_feature='sum_arrival_times') \
           .pipe(get_rolling_feature, history_years, exclude_days, rolling_func='count',   feature='VerschilAankomstEnStart', new_feature='num_arrival_times') \
           .pipe(get_feature_of_last_appointment, exclude_days, feature='no_show', new_feature='last_noshow') \
           .pipe(get_feature_of_last_appointment, exclude_days, feature='STARTDATEPLAN', new_feature='last_appointment_date') 
    
    # calculate percentage of no shows
    df['perc_no_shows'] = df['num_no_shows'].fillna(0) / df['num_appointments'].fillna(0) 
    df['perc_no_shows_spec'] = df['num_no_shows_spec'].fillna(0) / df['num_appointments_spec'].fillna(0)
    
    # calculate appointments at other location
    df['num_appointments_other_location'] = df['num_appointments'].fillna(0) - df['num_appointments_same_location'].fillna(0)
        
    # calculate mean arrival time
    df['stiptheid'] = df['sum_arrival_times'].fillna(0) / df['num_arrival_times'].fillna(0)
    df.loc[np.isinf(df['stiptheid']), 'stiptheid'] = np.nan
    
    # calculate days since the last appointment
    df['days_since_last_appointment'] = (df['STARTDATEPLAN'] - df['last_appointment_date']).dt.days

    return df

