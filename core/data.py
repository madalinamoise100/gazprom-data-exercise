# sqlalchemy imports
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Time, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists
from sqlalchemy import and_

# other imports
import pandas as pd
import glob
import os
import numpy as np
import ntpath
import datetime

# vars
db_string = 'postgresql+psycopg2://test_user:test_user@localhost:5432/gazprom_data_exercise'

# return data about meters
def get_meters():
    # connect
    try:
        db = create_engine(db_string)
        Session = sessionmaker(bind=db)
        s = Session()

    except Exception as e:
        raise Exception("Failed to connect to database - {}".format(e))

    try:
        df = pd.read_sql_query("select meter_id from all_meters",con=db)
        result = {}
        result['total'] = len(df.meter_id)
        result['meters'] = []
        for meter in df.meter_id:
            result['meters'].append(meter)
        return result

    except Exception as e:
        raise Exception("Failed to retrieve meter data - {}".format(e))

    try:
        s.close()

    except Exception as e:
        raise Exception("Failed to close database session - {}".format(e))

# return all data about a particular meter
def get_meter_data(meter_id):
    # connect
    try:
        db = create_engine(db_string)
        Session = sessionmaker(bind=db)
        s = Session()

    except Exception as e:
        raise Exception("Failed to connect to database - {}".format(e))

    try:
        df = pd.read_sql_query("select meter_id, recorded_at, consumption, file_id from smart_meter_data where meter_id='{}'".format(meter_id),con=db)
        result = {}
        total_consumption = 0
        result['data'] = {}
        for timestamp in df.recorded_at:
            consumption = df.loc[df.recorded_at == timestamp, 'consumption'].iloc[0]
            file_id = df.loc[df.recorded_at == timestamp, 'file_id'].iloc[0]
            total_consumption = total_consumption + consumption
            result['data'][str(timestamp)] = {}
            result['data'][str(timestamp)]['consumption'] = consumption
            result['data'][str(timestamp)]['file_id'] = file_id
        result['total_consumption'] = round(total_consumption, 2)
        return result

    except Exception as e:
        raise Exception("Failed to retrieve data for meter {} - {}".format(meter_id, e))

    try:
        s.close()

    except Exception as e:
        raise Exception("Failed to close database session - {}".format(e))

# return data about all files
def get_files():
    # connect
    try:
        db = create_engine(db_string)
        Session = sessionmaker(bind=db)
        s = Session()

    except Exception as e:
        raise Exception("Failed to connect to database - {}".format(e))

    try:
        df = pd.read_sql_query("select file_id, created_at from all_files",con=db)
        result = {}
        result['total'] = len(df.file_id)
        result['files'] = {}
        for file in df.file_id:
            result['files'][file] = df.loc[df.file_id == file, 'created_at'].iloc[0].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
        return result

    except Exception as e:
        raise Exception("Failed to retrieve file data - {}".format(e))

    try:
        s.close()

    except Exception as e:
        raise Exception("Failed to close database session - {}".format(e))

# return most recent file
def get_last_file():
    # connect
    try:
        db = create_engine(db_string)
        Session = sessionmaker(bind=db)
        s = Session()

    except Exception as e:
        raise Exception("Failed to connect to database - {}".format(e))

    try:
        df = pd.read_sql_query("select file_id, created_at from all_files",con=db)
        result = {}
        df['created_at'] = pd.to_datetime(df['created_at'])
        most_recent_timestamp = df['created_at'].max()
        for file in df.file_id:
            most_recent_file = df.loc[df.created_at == most_recent_timestamp, 'file_id'].iloc[0]
        result['most_recent_file'] = most_recent_file
        result['created_at'] = most_recent_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        return result

    except Exception as e:
        raise Exception("Failed to retrieve most recent file - {}".format(e))

    try:
        s.close()

    except Exception as e:
        raise Exception("Failed to close database session - {}".format(e))
