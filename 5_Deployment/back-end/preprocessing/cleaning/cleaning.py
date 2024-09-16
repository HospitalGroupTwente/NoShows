import pandas as pd
import numpy as np


def remove_specialism(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Removes emergency department appointments
    '''
    
    spec_to_remove = ['SEH', 'EHH', 'CCU', ]  
    df = df[ (~df['SPECCODE'].isin( spec_to_remove )) | (~df['TARAFD'].isin( spec_to_remove )) | (~df['CODE'].isin( spec_to_remove ))]
    spec_to_remove = ['RAD', 'APO', 'ONC', 'GEV', 'ORT', 'GGZ', 'PSY', 'ANE']   # these specs either had very few appointments or no shows, or induced a lot of noise
    df = df[~df['SPECIALISME'].isin(spec_to_remove)]

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
    (1) emergency department appointments (2) call consultations (3) appointments at other (external) locations
    '''
    
    df = remove_specialism(df)
    df = remove_call_consultations(df)
    df = remove_locations(df)
    df = df[df['INVOERDAT'] < df['STARTDATEPLAN']]   # removes appointments scheduled after or on the same day as appointment

    return df
