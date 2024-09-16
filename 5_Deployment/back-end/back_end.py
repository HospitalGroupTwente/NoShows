import pandas as pd
import numpy as np
import joblib
import pyodbc
import time
from datetime import datetime as dt
import sys
import schedule
import warnings
import yaml

sys.path.insert(0, '/app/preprocessing')
from preprocessing import *

warnings.filterwarnings("ignore")

def get_db_connection(driver, server, db, uid, password, port):
    conn = pyodbc.connect(f'Driver={driver};'
                          f'Server={server};'
                          f'Database={db};'
                          f'UID={uid};'
                          f'PWD={password};'
                          f'Port={port}')
    return conn

def days_from_today():
    '''
    Calculates the days from 3 workdays ahead
    Example: So 3 workdays from wednesday is monday, this is 5 days.
    It will return None when it is weekend.
    '''
    working_days = [3, 3, 5, 5, 5, None, None]
    day_index = time.localtime().tm_wday

    return(working_days[day_index])

def get_appointments_from_db(conn, n_years):
    n_days =  days_from_today()

    # if it is weekend
    if n_days is None:
        return None

    query = '''    
    -- get patients who have an appointment over 3 days
    WITH CTE_patient_numbers AS(
        SELECT DISTINCT 
        AFS.PATIENTNR
        
        FROM  [sql2019hix-h02].[HiX_OVZ].[dbo].[AGENDA_AFSPRAAK] AS AFS
        LEFT JOIN  [sql2019hix-h02].[HiX_OVZ].[dbo].CSZISLIB_LOCATION as loc on loc.LOCATIONID = AFS.LOCATIONID
        LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[RP_PLANOBJECT] AS RPP ON afs.AFSPRAAKNR = RPP.LINKEDOBJECTID   AND RPP.LINKEDOBJECTCLASSID = 'AGENDA_AFSPRAAK'     
        LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[AGENDA_SUBAGEND] AS SA  ON AFS.[AGENDA] = SA.[AGENDA]   AND AFS.[SUBAGENDA] = SA.[SUBAGENDA]      
        LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[AGENDA_SUBAGENPROD] SAP ON SAP.SUBAGENDA = SA.SUBAGENDA
        LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[AGENDA_AFSPCODE] AC ON AC.[CODE] = AFS.[CODE] AND AC.AGENDA = AFS.AGENDA
        LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[PATIENT_PATIENT] PP ON PP.PATIENTNR = AFS.PATIENTNR
                    
        WHERE 1 = 1   
        AND CAST(RPP.STARTDATEPLAN AS DATE) = CAST(DATEADD(DAY,''' + str(n_days) + ''', GETDATE())  AS DATE)
        AND AFS.PATIENTNR NOT LIKE '' 
        AND AC.CONSTYPE IN ('E','H','V','*')
    )

    SELECT DISTINCT 
    AFS.PATIENTNR
    ,PP.GESLACHT
    ,PP.POSTCODE
    ,PP.WOONPLAATS
    ,DATEDIFF(YEAR, PP.GEBDAT , RPP.STARTDATEPLAN) AS LEEFTIJD
    ,AFS.INVOERDAT
    ,RPP.STARTDATEPLAN
    ,RPP.STARTTIMEPLAN
    ,AFS.AANKOMST
    ,AFS.AGENDA
    ,SA.SPECCODE
    ,SA.TARAFD
    ,SPECIALISM = COALESCE(SA.SPECCODE, SA.TARAFD)
    ,AFS.LOCATIONID
    ,loc.[DESCRIPTION]
    ,AC.CONSTYPE
    ,AFS.[CODE]
    ,AFS.DUUR
    ,REDEN = MR.OMSCHR
    ,PP. GEBDAT
    ,TRIM('-' FROM CONCAT(MEISJESNAA, '-', ACHTERNAAM)) AS NAAM
    ,CASE WHEN AFS.[VOLDAAN] = 'J' THEN 'Ja' ELSE 'Nee' END AS [IsVoldaan]
        ,CASE WHEN COALESCE(RPP.STARTDATEPLAN, AFS.DATUM, '19000101') >= getdate() THEN 1
            WHEN ISNULL(NULLIF(AFS.[VOLDAAN],''),'') = '' THEN 2
            WHEN AFS.[VOLDAAN] = 'J' AND NULLIF(AFS.[AUTODAT],'') IS NULL THEN 3
            WHEN AFS.[VOLDAAN] = 'N' AND NULLIF(AFS.[AUTODAT],'') IS NULL THEN 4
            WHEN AFS.[VOLDAAN] = 'J' AND NOT NULLIF(AFS.[AUTODAT],'') IS NULL THEN 5
            WHEN AFS.[VOLDAAN] = 'N' AND NOT NULLIF(AFS.[AUTODAT],'') IS NULL THEN 6
            WHEN AFS.[VOLDAAN] = 'J' AND NOT NULLIF(AFS.[AUTODAT],'') IS NULL AND NULLIF(SAP.[AUTODAT],'') IS NOT NULL THEN 7
            WHEN AFS.[VOLDAAN] = 'N' AND NOT NULLIF(AFS.[AUTODAT],'') IS NULL AND NULLIF(SAP.[AUTODAT],'') IS NOT NULL THEN 8
            ELSE -1
    END AS AfspraakstatusKey 
    , CASE WHEN (HEADER LIKE '%verloskund%' OR FOOTER LIKE '%verloskund%' OR RPPR.LOCATION = 'ZH00007689') THEN 1 ELSE 0 END AS VERLOSKUNDE
    , CASE WHEN (SA.OMSCHR LIKE '%physician assistant%')                   THEN 1 ELSE 0 END AS PA

    FROM  [sql2019hix-h02].[HiX_OVZ].[dbo].[AGENDA_AFSPRAAK] AS AFS
    LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].CSZISLIB_LOCATION as loc on loc.LOCATIONID = AFS.LOCATIONID
    LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[RP_PLANOBJECT] AS RPP ON afs.AFSPRAAKNR = RPP.LINKEDOBJECTID   AND RPP.LINKEDOBJECTCLASSID = 'AGENDA_AFSPRAAK'     
    LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[AGENDA_SUBAGEND] AS SA  ON AFS.[AGENDA] = SA.[AGENDA]   AND AFS.[SUBAGENDA] = SA.[SUBAGENDA]               
    LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[AGENDA_SUBAGENPROD] SAP ON SAP.SUBAGENDA = SA.SUBAGENDA
    LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[AGENDA_AFSPCODE] AC ON AC.[CODE] = AFS.[CODE] AND AC.AGENDA = AFS.AGENDA
    LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[PATIENT_PATIENT] PP ON PP.PATIENTNR = AFS.PATIENTNR
    LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[AGENDA_MUTATIEREDEN] MR ON AFS.VERPLREDEN = MR.Code
    LEFT JOIN [sql2019hix-h02].[HiX_OVZ].[dbo].[RP_PROGRAM] RPPR ON RPP.PROGRAM = RPPR.ID   -- voor verloskunde uitsluiting

    RIGHT JOIN CTE_patient_numbers PATS ON PATS.PATIENTNR = AFS.PATIENTNR  -- Right join op CTE, we willen alleen de historie van de patienten die een afspraak hebben gepland komende drie dagen

    WHERE 1 = 1  
    AND SA.SUBAGENDA NOT IN ('002306', 'ZH0338', 'ZH1709')   -- geboorte agenda's
    AND CAST(RPP.STARTDATEPLAN AS DATE) <= CAST(DATEADD(DAY,''' + str(n_days) + ''', GETDATE())  AS DATE)  
    AND RPP.STARTDATEPLAN >= DATEADD(YEAR, -'''+ str(n_years) +''', GETDATE())
    AND AFS.PATIENTNR NOT LIKE '' 
    AND AC.CONSTYPE IN ('E','H','V','*')
    '''

    results = pd.read_sql_query(query, conn)
    return results

def convert_features_to_category(df):
    '''Converts categorical feature to pandas category type'''
    
    for col in ['GESLACHT', 'POSTCODE', 'WOONPLAATS', 'AGENDA', 'DESCRIPTION', 'CONSTYPE', 'CODE', 'SPECIALISME', 'LOCATIE', 'DagAfspraak', 'AfspraakZelfdeDag']:
        df[col] = df[col].astype('category')
    
    return df

def attach_predictions(df_original, df_preds):
    df_display = df_original.merge(df_preds, on=['PATIENTNR', 'SPECIALISME'], how='right')  # right join, we only want to display patients with predictions

    return df_display

def preprocess(df):

    df['SPECIALISME'] = df['SPECCODE'].combine_first(df['TARAFD'])
    df['STARTDATEPLAN'] = pd.to_datetime(df['STARTDATEPLAN'])
    df = preprocess_noshow_data(df, hist_years=10, n_appointments=0, training=False)

    return df

def apply_model(data_entry):
    pred = model.predict_proba(misc.get_feature_df(data_entry, training=False))[:,1]
    return pred

def process_gyn(df):
    # filters out verloskunde 
    df = df[((df['AGENDA'] == 'CSA006') & (df['VERLOSKUNDE'] == 0)) | (df['AGENDA'] != 'CSA006')]
    # filter out PA
    df = df[((df['AGENDA'] == 'CSA006') & (df['PA'] == 0)) | (df['AGENDA'] != 'CSA006')]

    return df


def predict(df):

    # get only appointments of the prediction date
    pred_date = df['STARTDATEPLAN'].max()  # get the date of which we want the predictions
    df_predict = df[df['STARTDATEPLAN'] == pred_date]

    # predict
    df_predict = process_gyn(df_predict)
    df_predict = convert_features_to_category(df_predict)
    df_predict = df_predict[df_predict['AGENDA'].isin(['CSA006', 'CSA009'])]   # agenda GYN, KIN (only for pilot, delete after)
    df_predict.loc[df_predict['num_no_shows'].isna(), 'num_no_shows'] = 0
    df_predict.loc[df_predict['perc_no_shows'].isna(), 'perc_no_shows'] = 0
    df_predict.loc[df_predict['last_noshow'].isna(), 'last_noshow'] = 0
    df_predict.loc[df_predict['num_appointments'].isna(), 'num_appointments'] = 0

    df_predict['PREDICTIE'] =  model.predict_proba(misc.get_feature_df(df_predict, training=False))[:,1]
    df_predict = df_predict.sort_values(by='PREDICTIE', ascending=False)
    
    return   df_predict[['PATIENTNR', 'NAAM', 'GEBDAT', 'GESLACHT', 'STARTDATEPLAN', 'STARTTIMEPLAN', 'SPECIALISM', 'PREDICTIE', 'LOCATIE']]

def add_phone_numbers(df, phone_numbers):
    '''
    Adds phone numbers to the dataframe. 
    One person can have multiple phone numbers.
    It will add all the phone numbers seperated by a semicollumn in one column.
    The front-end can extract the phone numbers by splitting on the semicollumn. 
    '''
    # doing this is a bit tedeous, so the approach might be a bit messy :)
    # it creates a list of all of the phone numbers and then joins them seperated by a semicollumn
    phone_numbers = phone_numbers.groupby('PATIENTNR')['TELEFOON'].apply(list).reset_index()  
    phone_numbers['TELEFOON'] = phone_numbers['TELEFOON'].apply(lambda x: '; '.join(x))

    # add to df
    df = pd.merge(df, phone_numbers[['PATIENTNR', 'TELEFOON']], on='PATIENTNR')

    return df

def get_phone_numbers(conn, df):
    '''
    Gets the phone numbers of each patient.
    It deletes the patients where no phone number is registered.
    '''
    patnumbers = ','.join([f"'{str(num)}'" for num in df['PATIENTNR'].tolist()])  # format so that it can be inserted into the query

    # query for patients
    query = f"""SELECT DISTINCT
    RC.ADRSID AS PATIENTNR
  
    ,CASE
    WHEN ADRTYPE = '0000000002' THEN 'Mobiel'
    WHEN ADRTYPE = 'CS00000004' THEN 'VastTelNr'
    END AS Type_Contactgegevens
    ,UITVOERDL AS TELEFOON
    ,OPMERKING
    FROM [SQL2019HIX-H02].[HiX_OVZ].[dbo].[ROUTEER_RCONTACT] RC
    INNER JOIN [SQL2019HIX-H02].[HiX_OVZ].[dbo].[ROUTEER_RADRES] RR ON RR.CONTACT = RC.RAID
    WHERE
    ADRTYPE IN ('0000000002','CS00000004')
    AND (UITVOERDL NOT LIKE '')
    AND RC.ADRSID IN ({patnumbers})"""

    phone_numbers = pd.read_sql_query(query, conn)
    phone_numbers['TELEFOON'] = phone_numbers.apply(lambda row: f"{row['TELEFOON']} ({row['OPMERKING']})".replace('()', '').replace('((', '(').replace('))', ')'), axis=1) 
    # select only patients from who a phone number is known
    df = df[df['PATIENTNR'].isin(phone_numbers['PATIENTNR'].tolist())]

    # attach phone numbers to df
    df = add_phone_numbers(df, phone_numbers)
    
    return df

def assign_groups(df, spec):
    '''
    Assigns group for AB-testing
    One group gets 'Intervention' the other 'Control'.
    This is done by alternating the top 20 from 'Intervention' to 'Control'
    '''

    # okay so we do not want the number 1 prediction to always be in a certain group
    # that is why I am alternating the number 1 each day by using the weekday number
    groups = ['Intervention', 'Control']
    if dt.now().weekday() % 2==0:
        groups = groups[::-1]  # reverses the list
    
    # if we have 20 or more we can add the group alternatively to the dataframe
    if len(df[df['SPECIALISM'] == spec]) >= 20:
        df_top = df[df['SPECIALISM'] == spec].iloc[0:20]
        df.loc[df_top.index, 'GROUP_AB'] = groups * 10
        df.loc[df['GROUP_AB'].isna(), 'GROUP_AB'] = None
    
    # else just assign all the rows we do have 
    else:
        for i in range(len(df[df['SPECIALISM'] == spec])):
            group = groups[0] if i%2==0 else groups[1]
            index =  df.loc[df['SPECIALISM'] == spec].index[i]
            df.loc[index, 'GROUP_AB'] = group
    
    return df


def write_results_to_db(conn, data):

    cursor = conn.cursor()

    # write data of previous day (together with action/status of calls) into the _all table
    cols = ",".join([str(i) for i in data.columns.tolist() + ['GEBELD', 'STATUS']])
    sql = f"INSERT INTO NoShowPreds_all ({cols}) SELECT {cols} FROM NoShowPreds_curr"
    cursor.execute(sql)
    conn.commit()
    cursor.execute("DELETE FROM NoShowPreds_curr")

    # early exit if there is no data, nothing to write
    if data is None:
        return

    # insert df records one by one. 
    cols = ",".join([str(i) for i in data.columns.tolist()])
    for i, row in data.iterrows():
        sql = f"INSERT INTO NoShowPreds_curr ({cols}) VALUES ({','.join(['?' for _ in range(len(row))])})"
        cursor.execute(sql, tuple(row))
    
    # commit and close cursor
    conn.commit()
    cursor.close()

def empty_db_table(conn):
    cursor = conn.cursor()

    # write data of previous day (together with action/status of calls) into the _all table
    # this only needs to be done on Friday, Try/except for Saturday if it maybe fails due to an empty database
    try:
        cols = ",".join([str(i) for i in data.columns.tolist() + ['GEBELD', 'STATUS']])
        sql = f"INSERT INTO NoShowPreds_all ({cols}) SELECT {cols} FROM NoShowPreds_curr"
        cursor.execute(sql)
        conn.commit()
    except Exception:
        pass

    cursor.execute("DELETE FROM NoShowPreds_curr")
    conn.commit()

    cursor.close()



def main(config, tries=0):
    try:
        print(f'Start prediction at {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}')
        con = get_db_connection(config['driver'], config['server'], config['database'], config['user'], config['password'], config['port'])
        
        df_appointments = get_appointments_from_db(con, 10)
        # if it is weekend or there are no appointments that are scheduled
        if df_appointments is None:
            empty_db_table(con)
        else:
            df_preprocessed = preprocess(df_appointments)
            df_predicted = predict(df_preprocessed)
            df_tel = get_phone_numbers(con, df_predicted)
            
            df_final = assign_groups(df_tel, spec='GYN')
            df_final = assign_groups(df_final, spec='KIN')
            write_results_to_db(con, df_final)
        

        con.close()
        print(f'Finished prediction at {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}')
    
    # if it somehow fails try again (maybe due to inability to connect to db)
    # tries for a maximum of 10 times
    except Exception as e:
        print("Failed to predict, trying again...")
        print(f"Error: {e}")
        if tries < 10:
            main(tries+1)


if __name__ == '__main__':
    # load config 
    config_path = './config.yaml' 
    with open(config_path) as stream:
        config = yaml.safe_load(stream)
    # load models in memory
    model = joblib.load('/app/py/no_show_model_v2.joblib')  

    # summer time: 02:00 is 00:00 on machine.
    # to prevent update issues, set schedule time to 04:00, so 02:00 machine time
    # main(config)
    schedule.every().day.at("02:00").do(main, config)

    print("Script started")
    while True:
        schedule.run_pending()
        time.sleep(10)
        
    