import requests
import pandas as pd
from sqlalchemy import create_engine

def extract()-> dict:
    """ API design to extract data of all available facilities of NASA
    Source url : https://data.nasa.gov/resource/gvk9-iz74.json
    Format : JSON
    """
    API_URL = "https://data.nasa.gov/resource/gvk9-iz74.json"
    data = requests.get(API_URL).json()
    return data

def transform(data:dict) -> pd.DataFrame:
    """ Transforming the dataset into desired structure and apply required filters
        The goal is to find active NASA facilities in california with last updated date 
    """
    df = pd.DataFrame(data)
    print(f"Total no of facilities available {len(data)}")
    df = df[df["state"].str.contains("CA")]
    df = df[df["status"].str.contains("Active")]
    print(f"Number of Active Facilities in california {len(df)}")
    df['last_update'] = df['last_update'].str.replace("(T).*","")
    df['occupied'] = df['occupied'].str.replace("(T).*","")
    df['record_date'] = df['record_date'].str.replace("(T).*","")
    return df

def load(df:pd.DataFrame)-> None:
    """ Loads data into a database
        Here using sqlite, but it could be any relational or nosql databases 
    """
    disk_engine = create_engine('sqlite:///nasa_facilities_ca.db')
    df.to_sql('nasa_ca', disk_engine, if_exists='replace')

data = extract()
df = transform(data)
load(df)
