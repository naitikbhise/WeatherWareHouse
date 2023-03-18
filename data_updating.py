import time
import requests
import psycopg2
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import *
from datetime import datetime, timedelta
from data_ingestion import *

# List of locations
locations = [{'location':"London",'latitude':51.5072,'longitude':0.1276},
            {'location':"Stockholm",'latitude':59.3293, 'longitude':18.0686},
            {'location':"Dubai",'latitude':25.2048, 'longitude':55.2708},
            {'location':"Mumbai",'latitude':19.0760, 'longitude':72.8777},
            {'location':"Hong Kong",'latitude':22.3193, 'longitude':114.1694},
            {'location':"San Francisco",'latitude':37.7749, 'longitude':122.4194},
            {'location':"Toronto",'latitude':43.6532, 'longitude':79.3832},
            {'location':"New York",'latitude':40.7128, 'longitude':74.0060},
            {'location':"Zurich",'latitude':47.3769, 'longitude':8.5417},
            {'location':"Singapore",'latitude':1.3521, 'longitude':103.8198}]

# Meteomatics credentials
username = 'unemployed_bhise'
password = '78iaBYGq8N'
    
# username:postgres; password :bhise at localhost port 5432. Also starting of SQL engine
postgres_conn_string = 'postgresql+psycopg2://postgres:bhise@localhost:5432/weatherdb'
engine = create_engine(postgres_conn_string)
Session = sessionmaker(bind=engine)

# Forever running while loop
while True:
    # Check Time
    now = datetime.now() - timedelta(hours=1)
    nextday = now + timedelta(days=7)
    formatted_date = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    nextformatted_date = nextday.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    data = []
    # Retreive Data rfom api meteomatics
    for location in locations:
        url = 'https://api.meteomatics.com/'+formatted_date+'--'+nextformatted_date+':PT1H/t_2m:C,precip_1h:mm,wind_speed_10m:ms/'+ str(location['latitude']) + ',' + str(location['longitude'])+'/json'
        response = requests.get(url, auth=(username, password))

        if response.ok:
            data.append(response.json)
            # process the data here
        else:
            print(f'Request failed with status code {response.status_code}')
    i = 0
    new_session = Session()
    for location in locations:
        """
            Here we take the json objects obtained from the meteomatics API and construct schema class items for ingesting into database.
        """
        print(" Updating Temperature Table ")
        for item in data[i]()['data'][0]['coordinates'][0]['dates']:
            row = {}
            row["location"] = location["location"]
            row["latitude"] = location["latitude"]
            row["longitude"] = location["longitude"]
            row["date"] = item['date'].split('T')[0]
            row["hour"] = int(item['date'].split('T')[1].split(":")[0])
            row["temperature"] = item['value']
            existing_row = new_session.query(TemperatureData).filter(TemperatureData.location == row["location"]).filter(TemperatureData.date == row['date']).filter(TemperatureData.hour == row['hour']).one_or_none()
            if existing_row and existing_row.temperature != item['value']:
                new_session.delete(existing_row)
                existing_row = None
            if existing_row is None:
                row = TemperatureData(**row)
                new_session.add(row)


        print(" Updating Precipitation Table ")
        for item in data[i]()['data'][1]['coordinates'][0]['dates']:
            row = {}
            row["location"] = location["location"]
            row["latitude"] = location["latitude"]
            row["longitude"] = location["longitude"]
            row["date"] = datetime.strptime(item['date'].split('T')[0], '%Y-%m-%d').date()
            row["hour"] = int(item['date'].split('T')[1].split(":")[0])
            row["preciipitation"] = item['value']
            existing_row = new_session.query(PrecipitationData).filter(PrecipitationData.location == row["location"]).filter(PrecipitationData.date == row['date']).filter(PrecipitationData.hour == row['hour']).one_or_none()  
            if existing_row and existing_row.preciipitation != item['value']:
                new_session.delete(existing_row)
                existing_row = None
            if existing_row is None:
                row = PrecipitationData(**row)
                new_session.add(row)

        print(" Updating Wind Table ")
        for item in data[i]()['data'][2]['coordinates'][0]['dates']:
            row = {}
            row["location"] = location["location"]
            row["latitude"] = location["latitude"]
            row["longitude"] = location["longitude"]
            row["date"] = datetime.strptime(item['date'].split('T')[0], '%Y-%m-%d').date()
            row["hour"] = int(item['date'].split('T')[1].split(":")[0])
            row["wind"] = item['value']
            existing_row = new_session.query(WindData).filter(WindData.location == row["location"]).filter(WindData.date == row['date']).filter(WindData.hour == row['hour']).one_or_none()  
            if existing_row and existing_row.wind != item['value']:
                new_session.delete(existing_row)
                existing_row = None
            if existing_row is None:
                row = WindData(**row)
                new_session.add(row)

        # Wide Data
        print(" Updating Wide Table ")
        for item in data[i]()['data'][0]['coordinates'][0]['dates']:
            row = {}
            row["location"] = location["location"]
            row["date"] = item['date'].split('T')[0]
            row["hour"] = int(item['date'].split('T')[1].split(":")[0])
            row["temperature"] = item['value']
            existing_row = new_session.query(WideData).filter(WideData.location == row["location"]).filter(WideData.date == row['date']).filter(WideData.hour == row['hour']).one_or_none()  
            if existing_row is None:
                row["preciipitation"] = None
                row["wind"] = None
                row = WideData(**row)
                new_session.add(row)
            else:
                existing_row.temperature = row["temperature"]
                new_session.merge(existing_row)


        for item in data[i]()['data'][1]['coordinates'][0]['dates']:
            row = {}
            row["location"] = location["location"]
            row["date"] = item['date'].split('T')[0]
            row["hour"] = int(item['date'].split('T')[1].split(":")[0])
            row["preciipitation"] = item['value']
            existing_row = new_session.query(WideData).filter(WideData.location == row["location"]).filter(WideData.date == row['date']).filter(WideData.hour == row['hour']).one_or_none()  
            if existing_row is None:
                row["temperature"] = None
                row["wind"] = None
                row = WideData(**row)
                new_session.add(row)
            else:
                existing_row.preciipitation = row["preciipitation"]
                new_session.merge(existing_row)

        for item in data[i]()['data'][2]['coordinates'][0]['dates']:
            row = {}
            row["location"] = location["location"]
            row["date"] = item['date'].split('T')[0]
            row["hour"] = int(item['date'].split('T')[1].split(":")[0])
            row["wind"] = item['value']
            existing_row = new_session.query(WideData).filter(WideData.location == row["location"]).filter(WideData.date == row['date']).filter(WideData.hour == row['hour']).one_or_none()  
            if existing_row is None:
                row["temperature"] = None
                row["preciipitation"] = None
                row = WideData(**row)
                new_session.add(row)
            else:
                existing_row.wind = row["wind"]
                new_session.merge(existing_row)

        # Long Data
        print(" Updating Long Table ")
        for item in data[i]()['data'][0]['coordinates'][0]['dates']:
            row = {}
            row["location"] = location["location"]
            row["date"] = item['date'].split('T')[0]
            row["hour"] = int(item['date'].split('T')[1].split(":")[0])
            row["Variable"] = "temperature"
            row["Value"] = item['value']
            existing_row = new_session.query(LongData).filter(LongData.location == row["location"]).filter(LongData.date == row['date']).filter(LongData.hour == row['hour']).filter(LongData.Variable == row['Variable']).one_or_none() 
            if existing_row is None:
                row = LongData(**row)
                new_session.add(row)
            else:
                existing_row.Value = row["Value"]
                new_session.merge(existing_row)

        for item in data[i]()['data'][1]['coordinates'][0]['dates']:
            row = {}
            row["location"] = location["location"]
            row["date"] = item['date'].split('T')[0]
            row["hour"] = int(item['date'].split('T')[1].split(":")[0])
            row["Variable"] = "preciipitation"
            row["Value"] = item['value']
            existing_row = new_session.query(LongData).filter(LongData.location == row["location"]).filter(LongData.date == row['date']).filter(LongData.hour == row['hour']).filter(LongData.Variable == row['Variable']).one_or_none()  
            if existing_row is None:
                row = LongData(**row)
                new_session.add(row)
            else:
                existing_row.Value = row["Value"]
                new_session.merge(existing_row)

        for item in data[i]()['data'][2]['coordinates'][0]['dates']:
            row = {}
            row["location"] = location["location"]
            row["date"] = item['date'].split('T')[0]
            row["hour"] = int(item['date'].split('T')[1].split(":")[0])
            row["Variable"] = "wind"
            row["Value"] = item['value']
            existing_row = new_session.query(LongData).filter(LongData.location == row["location"]).filter(LongData.date == row['date']).filter(LongData.hour == row['hour']).filter(LongData.Variable == row['Variable']).one_or_none()  
            if existing_row is None:
                row = LongData(**row)
                new_session.add(row) 
            else:
                existing_row.Value = row["Value"]
                new_session.merge(existing_row)
        i+=1
    new_session.commit()
    # Sleep for an hour before resuming
    time.sleep(3600)
    
