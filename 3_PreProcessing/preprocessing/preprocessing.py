import pandas as pd
import sys

from features import datetime, geographic, cumulative, misc
from cleaning import cleaning



def preprocess_noshow_data(df_data: pd.DataFrame, start_year, hist_years: int, training: bool = False) -> pd.DataFrame:
    '''
    Preprocesses all of the data so that it can be used for training or inference

    Paramters
    ---------
    df_data :  pd.DataFrame
        Dataframe containing all the data
    start_year : str
        Year from which to start the preprocessing (format: yyyy-mm-dd)
        Everything below that date will be used as history to calculate the cumulative features
    hist_years : int
        Number of years which will be used as the history dataset of each patient to calculate the cumulative features
    n_appointments : int
        Treshold for number of appointments to be in the dataset
        If a record contains a value below the treshold, it will be discarded 
    training : bool
        Boolean to indicate if the preprocessing is for training
        If training=True the target variable is attached to the dataframe
        If training=False the target variable is not attached to the dataframe
    
    Returns
    -------
    pd.DataFrame
        Dataframe containing the preprocessed data
        Only the features for modelling are returned
    '''
    # removing data not suitable for prediction
    df_data = cleaning.clean_data(df_data)
    df_data = df_data[df_data['CONSTYPE'].isin(['H', 'E', 'V', '*'])]

    # get target variable
    df_data = misc.process_target_variable(df_data)  

    # get date features
    df_data['AfspraakZelfdeDag'] = datetime.has_appointment_same_day(df_data)
    df_data['VerschilInplannenEnAfspraak'] = datetime.difference_scheduling_and_appointment(df_data)
    df_data = df_data[df_data['VerschilInplannenEnAfspraak']  >= 3]   # at inference we are predicting appointments over 3 days
    df_data['MaandAfspraak'] = datetime.fetch_month(df_data['STARTDATEPLAN'])
    df_data['DagAfspraak'] = datetime.fetch_weekday(df_data['STARTDATEPLAN'])
    df_data['TijdAfspraak'] = datetime.fetch_appointment_hour(df_data['STARTTIMEPLAN'])
    df_data['SPECIALISME'] = misc.get_specialism(df_data)
    df_data['VerschilAankomstEnStart'] = datetime.difference_scheduling_and_arrival(df_data, treshold=60)

    # get geo features
    df_data['POSTCODE'] = geographic.extract_zipcode(df_data['POSTCODE'])
    df_data['LOCATIE'] = df_data['DESCRIPTION'].apply(geographic.get_location)
    zip_codes = geographic.get_all_nl_zip_codes('/export/home/jmaathuis/Documents/NO-SHOWS/3_PreProcessing/NL(1).txt')
    df_data = df_data.merge(zip_codes, how='left', left_on='POSTCODE', right_index=True)
    df_data['AFSTAND'] = geographic.haversine_distance(df_data['LOCATIE'], df_data['latitude'], df_data['longitude'])

    # get cumulative features
    df_data = cumulative.calculate_cum_features(df_data, history_years=hist_years, exclude_days=3)
    # df_data = df_data[df_data['STARTDATEPLAN'] >= start_year]

    df_data = df_data[df_data['CONSTYPE'].isin(['H', 'E', 'V', '*'])]
    return df_data
    # return misc.get_feature_df(df_data, training=training)
    

if __name__ == '__main__':
    # case for creating a training setepr
    file = sys.argv[1]
    start_year = str(sys.argv[2])
    history_years = int(sys.argv[3])
    # appointments = int(sys.argv[4])

    outfile = f'{file.split(".csv")[0]}_start_date={start_year}_hist={history_years}_improved2.csv'
    print(outfile)

    df = pd.read_csv(file, sep=';', parse_dates=['STARTDATEPLAN', 'INVOERDAT'], 
                dtype={'PATIENTNR': 'int64', 'MERGED': 'int64', 'GESLACHT': str, 'POSTOCDE': str, 'WOONPLAATS': str, #'LEEFTIJD': 'int64', 
                        'STARTTIMEPLAN': str, 'AANKOMST': str, 'AGENDA': str, 'SUBAGENDA': str, 'SPECCODE': str, 'TARAFD': str,
                        'LOCATIONID': str, 'DESCRIPTION': str, 'IsVoldaan': str, 'AfspraakstatusKey': 'Int64', 'CONSTYPE': str, 'CODE': str,
                        'DUUR': pd.Int64Dtype()}, encoding='utf-8-sig')

    df_pp = preprocess_noshow_data(df, start_year=start_year, hist_years=history_years, training=True)

    df_pp.to_csv(outfile, index=0)


