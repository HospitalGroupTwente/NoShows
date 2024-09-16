import pandas as pd
import numpy as np

# old function, this was very slow since an apply needed to be utilized
# this function is improved in a new function down below
def calculate_cum_features_old(row: pd.Series, data: pd.DataFrame, n_years: int) -> pd.Series:
    '''
    Calculates the cumalutive features for each appointment
    This is based on a history of the patient of n years ago

    The features consists out of:
    (1) number of no shows (2) number of appointments (3) percentage of no shows 
    (4) mean difference between arrival and appointment time (5) days since last appointment
    (6) appointment last week y/n
    '''
    patnr = row.PATIENTNR
    end_date = pd.to_datetime(row.STARTDATEPLAN)
    start_date = end_date - pd.DateOffset(years=n_years)
    spec = row.SPECIALISME

    # get history of that patient from the past n years
    data = data[ (data['PATIENTNR'] == patnr) & (data['STARTDATEPLAN'] >= start_date) & (data['STARTDATEPLAN'] < end_date) ]
    data_spec = data[data['SPECIALISME'] ==  spec]
    
    # if there is no history of patient
    if len(data) == 0:
        return pd.Series({f'num_no_shows': 0, f'perc_no_shows': np.nan, f'num_appointments': 0, f'num_no_shows_spec': 0, f'perc_no_shows_spec': np.nan, f'num_appointments_spec': 0,  f'stiptheid': np.nan, f'days_since_last_appointment': 0, f'no_show_last_appointment': 0, f'appointment_last_week': 0, f'perc_scaled': np.nan, f'weighted_no_show_percentage' : np.nan})

    # calculate cumulative features
    num_no_shows = data['no_show'].sum()  # number of no shows
    num_appointments = len(data)  # number of appointments
    perc_no_shows = num_no_shows / num_appointments  # percentage of no shows
    stiptheid = data['VerschilAankomstEnStart'].mean()  # stiptheid
    days_since_last_appointment = (end_date - max(data['STARTDATEPLAN'])).days
    no_show_last_appointment = data[data['STARTDATEPLAN'] == max(data['STARTDATEPLAN'])]['no_show'].values[0]
    appointment_last_week = 1 if days_since_last_appointment <= 7 else 0

    num_no_shows_spec = data_spec['no_show'].sum()  # number of no shows
    num_appointments_spec = len(data_spec)  # number of appointments
    perc_no_shows_spec = num_no_shows / num_appointments  # percentage of no shows
    perc_scaled = perc_no_shows * num_appointments

   

    return pd.Series({f'num_no_shows': num_no_shows, f'perc_no_shows': perc_no_shows, f'num_appointments_spec': num_appointments_spec, f'num_no_shows_spec': num_no_shows_spec, f'perc_no_shows_spec': perc_no_shows_spec, f'num_appointments': num_appointments, f'stiptheid': stiptheid,  f'days_since_last_appointment': days_since_last_appointment, f'no_show_last_appointment': no_show_last_appointment, f'appointment_last_week': appointment_last_week, f'perc_scaled': perc_scaled, f'weighted_no_show_percentage': weighted_no_show_percentage})

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


    # create rolling windows, df_exclude will be used to remove the data about appointments of the previous n days. This is done because in deployment we will be predicting no shows over n days
    window_days = 365 * history_years
    df_period = df.reset_index().set_index('STARTDATEPLAN').groupby('PATIENTNR', sort=False)[['no_show', 'VerschilAankomstEnStart']].rolling(f'{window_days}D')
    df_exclude = df.reset_index().set_index('STARTDATEPLAN').groupby('PATIENTNR', sort=False)[['no_show', 'VerschilAankomstEnStart']].rolling(f'{exclude_days}D')

    # calculate previous no show features
    ## calculate the previous number of no shows per appointment in the last n years
    df_num_no_shows = (df_period['no_show'].sum() - df_exclude['no_show'].sum()).reset_index().rename(columns={'no_show': 'num_no_shows'})
    df = df.merge(df_num_no_shows, on=['STARTDATEPLAN', 'PATIENTNR'])

    ## calculate the previous number of appointments per appointment in the last n years
    df_num_appointments = (df_period['no_show'].count() - df_exclude['no_show'].count()).reset_index().rename(columns={'no_show': 'num_appointments'})
    df = df.merge(df_num_appointments, on=['STARTDATEPLAN', 'PATIENTNR'])

    ## calculate percentage of previous no shows
    df['perc_no_shows'] = df['num_no_shows'] / df['num_appointments']


    # calcualte mean time of arrival 
    df_stipteid = (df_period['VerschilAankomstEnStart'].sum() - df_exclude['VerschilAankomstEnStart'].sum().fillna(0)).reset_index().rename(columns={'VerschilAankomstEnStart': 'stiptheid'})
    df = df.merge(df_stipteid, on=['STARTDATEPLAN', 'PATIENTNR'])
    df['stiptheid'] = df['stiptheid'] - df['num_appointments']


    # calculate status of the last appointment, excluding the appointments from the last 3 days
    df = df.sort_values(by=['PATIENTNR', 'STARTDATEPLAN'])

    ## create a shifted df to use for merging
    df_shifted = df.copy()
    df_shifted['STARTDATEPLAN'] = df_shifted['STARTDATEPLAN'] + pd.Timedelta(days=exclude_days)
    df_shifted = df_shifted.rename(columns={'no_show': 'last_noshow'})

    ## perform an asof merge, this is a merge where it tries to find an match to join on and otherwise finds the closes possible value to merge on
    ## we use direction='backwards' to find the appointment most close to the date of appointment date minus 3 days (offset)
    df = pd.merge_asof(df.sort_values(by=['STARTDATEPLAN']), df_shifted[['PATIENTNR', 'STARTDATEPLAN', 'last_noshow']].sort_values(by=['STARTDATEPLAN']),
                        on='STARTDATEPLAN', by='PATIENTNR', direction='backward')


    # calculate time difference in days between this and previous appointemtn, excluding the appointments from the last 3 days
    df = df.sort_values(by=['PATIENTNR', 'STARTDATEPLAN'])

    ## create a shifted df to use for merging
    df_shifted = df.copy()
    df_shifted['DATE_PREV_APP'] = df_shifted['STARTDATEPLAN']
    df_shifted['STARTDATEPLAN'] = df_shifted['STARTDATEPLAN'] + pd.Timedelta(days=exclude_days)

    ## perform an asof merge, this is a merge where it tries to find an match to join on and otherwise finds the closes possible value to merge on
    ## we use direction='backwards' to find the appointment most close to the date of appointment date minus 3 days (offset)
    df = pd.merge_asof(df.sort_values(by=['STARTDATEPLAN']), df_shifted[['PATIENTNR', 'STARTDATEPLAN', 'DATE_PREV_APP']].sort_values(by=['STARTDATEPLAN']),
                        on='STARTDATEPLAN', by='PATIENTNR', direction='backward')
    df['days_since_last_appointment'] = (df['DATE_PREV_APP'] - df['STARTDATEPLAN']).dt.days

    return(df)

