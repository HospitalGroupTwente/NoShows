import pandas as pd
import numpy as np
from numpy import radians, sin, cos, sqrt, arctan2

def extract_zipcode(series: pd.Series) -> pd.Series:
    '''
    Extracts the 4 digits from a zipcode field
    '''
    return series.astype('str').str.extract(r'^(\d{4})').astype('float').astype('Int64', errors='ignore')


def get_location(description: str) -> float:
    '''
    Gets the location based on the appointment location description
    Returns either Hengelo, Almelo or nan
    '''

    # if it is NaN
    if type(description) == float:
        return np.nan
    
    desc = description.lower()
    if 'hengelo' in desc:
        return 'Hengelo'
    elif 'almelo' in desc:
        return 'Almelo'
    else:
        return np.nan


def get_all_nl_zip_codes(postalcodes_path: str) -> pd.DataFrame:
    ''' 
    Gets the longitudes and langitudes for all the postal codes from a text file
    Text file was obtained via: https://download.geonames.org/export/zip/
    '''

    zip_codes = pd.read_table(postalcodes_path, sep="\t",header=None,names=["country", "postalcode", "city", "admin_name1", "admin_code1",
                                    "admin_name2", "admin_code2", "admin_name3", "admin_code3", "latitude", "longitude", "accuracy",])
    zip_codes = zip_codes.set_index("postalcode")[["latitude", "longitude"]]
    zip_codes = zip_codes.loc[~zip_codes.index.duplicated()]

    return zip_codes


def haversine_distance(location: pd.Series, lats: pd.Series, lons: pd.Series) -> float:
    ''' 
    Calculates the haversine distance to the hospital, based on the patients coordinates 
    The hospital location can be either Hengelo or Almelo
    '''

    # get coords of hospital based on appointment location
    coords = pd.DataFrame([(52.263950, 6.77085), (52.337151, 6.64080)], index=['Hengelo', 'Almelo'], columns=['latitude', 'longitude'])
    coords = pd.merge(location.reset_index()['LOCATIE'], coords, how='left', left_on='LOCATIE', right_index=True)
    lats_zgt = coords['latitude'].to_numpy()
    lons_zgt = coords['longitude'].to_numpy()

    lat1 = radians(lats)
    lon1 = radians(lons)
    lat2 = radians(lats_zgt)
    lon2 = radians(lons_zgt)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # haversines formula
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    distance = 6378.137 * c

    return distance