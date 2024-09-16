'''Functions to process date time features'''

import pandas as pd
import numpy as np

def has_appointment_same_day(df: pd.DataFrame) -> pd.Series:
    ''' 
    Returns a boolean series indicating if that patient has more than one appointmet scheduled on that day
    '''
    return df[['PATIENTNR', 'STARTDATEPLAN']].duplicated()


def difference_scheduling_and_appointment(df: pd.DataFrame) -> pd.Series:
    '''
    Calculates the difference in days between scheduling day and appointment day
    '''

    delta = df['STARTDATEPLAN'] - df['INVOERDAT']
    day_delta = delta.dt.days

    return day_delta


def difference_scheduling_and_arrival(df: pd.DataFrame, treshold:int = 60) -> pd.Series:
    '''
    Calculate the difference in minutes between schedule time and arrival time
    A positive value indicates that the patient was on time
    A negative value indicates that the patient was too late
    Absoulte values larger than the treshold are being replaced to missing
    '''

    arrival = pd.to_datetime(df['AANKOMST'], format='%H:%M', errors='coerce')
    scheduled = pd.to_datetime(df['STARTTIMEPLAN'], format='%H:%M', errors='coerce')
    diff_minutes = (arrival - scheduled).dt.total_seconds() / 60
    diff_minutes.loc[abs(diff_minutes) > treshold] = np.nan

    return diff_minutes


def fetch_weekday(series: pd.Series) -> pd.Series:
    ''' 
    Gets the weekday of the given dates 
    '''
    return series.dt.strftime('%A')

def fetch_month(series: pd.Series) -> pd.Series:
    '''
    Gets the month of hte appointment date
    '''
    return series.dt.month


def fetch_appointment_hour(series: pd.Series) -> pd.Series:
    ''' 
    Fetches the hour of the appointment
    Format should be a string in format 'hh:mm'
    '''
    times = pd.to_datetime(series, format= '%H:%M')
    return times.dt.hour
