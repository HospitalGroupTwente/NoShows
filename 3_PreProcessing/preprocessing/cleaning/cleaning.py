import pandas as pd
import numpy as np


def remove_noisy_specialism(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Removes specialisms which introduces noise into the data.
    This is due to different reasons
    '''
    
    spec_to_remove = ['SEH', 'EHH', 'CCU', 'RAD', 'APO', 'ONC', 'GEV', 'ORT', 'GGZ', 'PSY', 'FYS', 'GER']  
    df = df[ (~df['SPECCODE'].isin( spec_to_remove )) & (~df['TARAFD'].isin( spec_to_remove )) & (~df['CODE'].isin( spec_to_remove ))]

    return df


def remove_noisy_appointment_codes(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Removes noisy appointments codes
    This can be due to different reasons, for example if it is a reminder for the doctor and not a real patient appointment
    '''
    
    code_to_remove = ['DIV', 'ADMIN', 'PRBPB', 'PRBSV']
    df = df[~df['CODE'].isin(code_to_remove)]   

    return df


def remove_call_consultations(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Removes call consultation appointments
    '''
    
    code_to_remove = ['HB' , 'TC', 'NB', '00001967']
    df = df[~df['CODE'].isin(code_to_remove)]   

    return df


def remove_locations(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Removes appointment at other locations that ZGT Hengelo or Almelo
    '''
    
    loc_keep = ['ZGT locatie Almelo', 'ZGT locatie Hengelo', 'Polikliniek Verloskunde Almelo', 'Obesitas centrum Hengelo ZGT', 'Oncologisch centrum Hengelo', 'Behandelcentrum Almelo', 'Slaapcentrum Hengelo', 'Behandelcentrum Hengelo', np.nan]
    df = df[df['DESCRIPTION'].isin(loc_keep)]

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    ''' 
    Removes data of which it does not make sense to predict no shows

    This consist of: 
    (1) noisy specialisms 
    (2) call consultations 
    (3) appointments at other (external) locations
    (4) appointments where the scheduling date is after the appointment date
    '''
    
    df = remove_noisy_specialism(df)
    df = remove_noisy_appointment_codes(df)
    df = remove_call_consultations(df)
    df = remove_locations(df)
    df = df[df['INVOERDAT'] < df['STARTDATEPLAN']]   # removes appointments scheduled after or on the same day as appointment

    return df
