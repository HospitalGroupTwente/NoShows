import pandas as pd

def get_specialism(df: pd.DataFrame) -> pd.Series:
    ''' 
    Cobmines two columns to get the specialism
    If SPECCODE is NULL it will use TARAFD
    '''
    specs = df['SPECCODE'].combine_first(df['TARAFD'])
    return specs

def process_target_variable(df: pd.DataFrame) -> pd.DataFrame:
    no_show_cats = ['Patient niet verschenen (of te laat gemeld)',
                'No show (geen factuur)',
                'Verzoek patient (<24 uur van tevoren afgemeld)',
    ]

    df = df[ ( df['AfspraakstatusKey'].isin([6, 8]) & (df['REDEN'].isin(no_show_cats)) ) | (~df['AfspraakstatusKey'].isin([6,8]))]
    df['no_show'] = df['AfspraakstatusKey'].isin([6, 8]).astype(int)   # convert to int
    

    return df

def get_feature_df(df: pd.DataFrame, training:bool) -> pd.DataFrame:
    '''
    Returns a dataframe with only the features.
    It also sorts the dataframe in the right format/sorted
    if training is set to true, the target variable will be added
    '''
    feature_cols = ['GESLACHT', 'LEEFTIJD', 'POSTCODE', 'WOONPLAATS', # patient features
                'AGENDA', 'DESCRIPTION', 'CONSTYPE', 'CODE', 'SPECIALISME','LOCATIE', 'DUUR', # appointment features
                'AfspraakZelfdeDag', 'AFSTAND', 'VerschilInplannenEnAfspraak', 'num_no_shows', 'perc_no_shows', 'stiptheid', 'MaandAfspraak', 'DagAfspraak', 'TijdAfspraak',  # engineerd features
                'num_appointments', 'last_noshow', 'days_since_last_appointment', # engineerd features
                ]
    # if you want to train with the data, add target variable and date
    if training:
        feature_cols.append('no_show')

    return df[feature_cols]
