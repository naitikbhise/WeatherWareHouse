from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import uvicorn

app = FastAPI()

# Permissible location List
location_list = ["Mumbai","Hong Kong","Stockholm","London","Toronto","Dubai","San Francisco","Zurich","Singapore","New York"]

# create the engine
postgres_conn_string = 'postgresql+psycopg2://postgres:bhise@localhost:5432/weatherdb'
engine = create_engine(postgres_conn_string)


@app.get('/locations')
async def get_locations():
    # Query the locations table and return the results as a list of dictionaries
    with engine.connect() as conn:
        query = 'SELECT DISTINCT location FROM locations_table'
        result = conn.execute(query)
        locations = [dict(row) for row in result]
    return locations

@app.get('/temperatures')
async def get_details_temp(payload: dict):
    query = 'SELECT * FROM temp_data'
    df = pd.read_sql_query(query, engine)
    df['date'] = pd.to_datetime(df["date"])
    today_date = datetime.now().date()
    hour = datetime.now().hour
    if payload is None:
        return df.to_dict(orient='records')
    if "location" in payload:
        df = df[df["location"].isin(payload["location"])]
        if len(df)==0:
            raise HTTPException(status_code=422, detail="Mention the 10 cities written in the Documentation.")
        df.reset_index(drop=True)
    if "date_start" not in payload:
        df = df[df['date']==today_date]
        df.reset_index(drop=True,inplace=True)
        return df[-1:].to_dict(orient='records')
    date_start = datetime.strptime(payload["date_start"] , '%Y-%m-%d')
    if "day_count" in payload:
        if payload["day_count"]>10:
            raise HTTPException(status_code=403, detail="Forecasting too much into future")
        if payload["day_count"]<1:
            raise HTTPException(status_code=400, detail="Bad data. try positive integer values.")
        date_end = date_start + timedelta(days=payload["day_count"])
        df = df[(df['date'] >= date_start) & (df['date'] < date_end)]
        if "statistics" in payload:
            if payload["statistics"]==1:
                new_df = df.groupby(["location","date"]).agg(temp_avg=("temperature", np.average),temp_max=("temperature", max),temp_min=("temperature", min)).reset_index()
                return new_df.to_dict(orient='records')
        return df.to_dict(orient='records')
    else:
        raise HTTPException(status_code=405, detail="Need the count of days")
    return df[-1:].to_dict(orient='records')

@app.get('/precipitations')
async def get_details_prec(payload: dict):
    query = 'SELECT * FROM precipitation_data'
    df = pd.read_sql_query(query, engine)
    today_date = datetime.now().date()
    hour = datetime.now().hour
    if payload is None:
        return df.to_dict(orient='records')
    if "location" in payload:
        df = df[df["location"].isin(payload["location"])]
        if len(df)==0:
            raise HTTPException(status_code=422, detail="Mention the 10 cities written in the Documentation.")
        df.reset_index(drop=True)
    if "date_start" not in payload:
        df = df[df["date"]==today_date]
        df.reset_index(drop=True,inplace=True)
        return df[-1:].to_dict(orient='records')
    date_start = datetime.strptime(payload["date_start"], '%Y-%m-%d').date()
    if "day_count" in payload:
        if payload["day_count"]>10:
            raise HTTPException(status_code=403, detail="Forecasting too much into future")
        if payload["day_count"]<1:
            raise HTTPException(status_code=400, detail="Bad data. try positive integer values.")
        date_end = datetime.now().date()+timedelta(days=payload["day_count"])
        df = df[(df['date'] >= date_start) & (df['date'] < date_end)]
        if "statistics" in payload:
            if payload["statistics"]==1:
                new_df = df.groupby(["location","date"]).agg(prec_avg=("preciipitation", np.average),prec_max=("preciipitation", max),prec_min=("preciipitation", min)).reset_index()
                return new_df.to_dict(orient='records')
        return df.to_dict(orient='records')
    else:
        raise HTTPException(status_code=405, detail="Need the count of days")
    return df[-1:].to_dict(orient='records')

@app.get('/winds')
async def get_details_wind(payload: dict):
    query = 'SELECT * FROM wind_data'
    df = pd.read_sql_query(query, engine)
    today_date = datetime.now().date()
    hour = datetime.now().hour
    if payload is None:
        return df.to_dict(orient='records')
    if "location" in payload:
        df = df[df["location"].isin(payload["location"])]
        if len(df)==0:
            raise HTTPException(status_code=422, detail="Mention the 10 cities written in the Documentation.")
        df.reset_index(drop=True)
    if "date_start" not in payload:
        df = df[df["date"]==today_date]
        df.reset_index(drop=True,inplace=True)
        return df.to_dict(orient="records")
    date_start = datetime.strptime(payload["date_start"], '%Y-%m-%d').date()
    if "day_count" in payload:
        if payload["day_count"]>10:
            raise HTTPException(status_code=403, detail="Forecasting too much into future")
        if payload["day_count"]<1:
            raise HTTPException(status_code=400, detail="Bad data. try positive integer values.")
        date_end = datetime.now().date()+timedelta(days=payload["day_count"])
        df = df[(df['date'] >= date_start) & (df['date'] < date_end)]
        if "statistics" in payload:
            if payload["statistics"]==1:
                new_df = df.groupby(["location","date"]).agg(wind_avg=("wind", np.average),wind_max=("wind", max),wind_max=("wind", min)).reset_index()
                return new_df.to_dict(orient="records")
        return df.to_dict(orient="records")
    else:
        raise HTTPException(status_code=405, detail="Need the count of days")
    return df[-1:].to_dict(orient="records")

@app.get('/averages')
async def get_averages(payload: dict):
    query = 'SELECT * FROM wide_data'
    df = pd.read_sql_query(query, engine)
    df['date'] = pd.to_datetime(df["date"])
    today_date = datetime.now().date()
    hour = datetime.now().hour
    if payload is None:
        return df.to_dict(orient='records')
    if "location" in payload:
        df = df[df["location"].isin(payload["location"])]
        if len(df)==0:
            raise HTTPException(status_code=422, detail="Mention the 10 cities written in the Documentation.")
        df.reset_index(drop=True)
    if "date_start" not in payload:
        df = df[df["date"]==today_date]
        df.reset_index(drop=True,inplace=True)
        return df.to_dict(orient='records')
    date_start = datetime.strptime(payload["date_start"], '%Y-%m-%d')
    if "day_count" in payload:
        if payload["day_count"]>10:
            raise HTTPException(status_code=403, detail="Forecasting too much into future")
        if payload["day_count"]<1:
            raise HTTPException(status_code=400, detail="Bad data. try positive integer values.")
        date_end = datetime.now()+timedelta(days=payload["day_count"])
        df = df[(df['date'] >= date_start) & (df['date'] < date_end)]
        if "statistics" in payload:
            if payload["statistics"]==1:
                new_df = df.groupby(["location","date"]).agg(wind_avg=("wind", np.average),temp_avg=("temperature", np.average),prec_avg=("preciipitation", np.average)).reset_index()
                return new_df.to_dict(orient='records')
        return df.to_dict(orient='records')
    else:
        raise HTTPException(status_code=405, detail="Need the count of days")
    return df[-1:].to_dict(orient='records')

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)