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
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sample_data/')
files = glob.glob(path + "/*.SMRT")
Base = declarative_base()
columns = ['record_identifier', 'meter_id', 'date', 'time', 'consumption', 'file_id']

class MeterReading(Base):
    __tablename__ = 'smart_meter_data'
    # __tablename__ = table_name
    id = Column(Integer, primary_key=True)
    meter_id = Column(String)
    recorded_at = Column(DateTime)
    consumption = Column(Float)
    file_id = Column(String)

    def __repr__(self):
        return "<MeterReading(meter_id='{}', recorded_at='{}', consumption='{}', file_id='{}')>"\
                .format(self.meter_id, self.date, self.time, self.consumption, self.file_id)

class Meters(Base):
    __tablename__ = 'all_meters'
    # __table_args__ = {"schema": "my_schema"}
    id = Column(Integer, primary_key=True)
    meter_id = Column(String)

    def __repr__(self):
        return "<MeterReading(meter_id='{}')>".format(self.meter_id)

class Files(Base):
    __tablename__ = 'all_files'
    # __table_args__ = {"schema": "my_schema"}
    id = Column(Integer, primary_key=True)
    file_id = Column(String)
    created_at = Column(DateTime)

    def __repr__(self):
        return "<MeterReading(file_id='{}', created_at='{}')>".format(self.file_id)

# check that header and footer exist and are in the right format
def validate_file(df, filename):
    try:
        # make this not hardcoded?
        for col in columns:
            if col == "record_identifier":
                if df.iloc[0][col] != "HEADR":
                    print("Missing header")
                    return False
                if df.iloc[-1][col] != "TRAIL":
                    print("Missing footer")
                    return False
            else:
                if type(df.iloc[0][col]) != str:
                    print("Invalid header format")
                    return False
                if not pd.isnull(df.iloc[-1][col]):
                    print("Invalid footer format")
                    return False
        return True
    except Exception as e:
        print("Failed to validate file {} - {}".format(filename, e))


db = create_engine(db_string)
# will not recreate existing tables
Base.metadata.drop_all(db)
Base.metadata.create_all(db, checkfirst=True)
Session = sessionmaker(bind=db)
s = Session()

for filename in files:
    df = pd.read_csv(filename, names=columns, index_col=None, dtype=object)
    if validate_file(df, ntpath.basename(filename)) == True:
        # get id of source file
        file_id = df.loc[df.record_identifier == "HEADR", 'file_id'][0]
        # get file creation timestamp
        # note that column names do not match because header has different format compared to the other rows
        creation_date = df.loc[df.record_identifier == "HEADR", 'time'][0]
        creation_time = df.loc[df.record_identifier == "HEADR", 'consumption'][0]
        print(file_id)

        # drop header, footer, file_id column
        df = df.iloc[1:, :]
        df = df.iloc[:-1]
        del df['file_id']

        # if file does not exist, add to all_files table
        if s.query(exists().where(Files.file_id == file_id)).scalar() == False:
            datetime_str = creation_date + " " + creation_time
            created_at = datetime.datetime.strptime(datetime_str, '%Y%m%d %H%M%S')
            new_file = Files(file_id=file_id, created_at=created_at)
            s.add(new_file)
            s.commit()
            # s.query(Files).first()

            for meter in df.meter_id.unique():
                # if meter does not exist, add to all_meters table
                if s.query(exists().where(Meters.meter_id == meter)).scalar() == False:
                    print("Recording new meter " + meter)
                    new_meter = Meters(meter_id=meter)
                    s.add(new_meter)
                    s.commit()

                # add data
                dates = df.loc[df.meter_id == meter, 'date']
                times = df.loc[df.meter_id == meter, 'time']

                for i in range(len(dates)):
                    datetime_str = dates.iloc[i] + " " + times.iloc[i]
                    recorded_at = datetime.datetime.strptime(datetime_str, '%Y%m%d %H%M%S')

                    # if a reading for this meter recorded at the same timestamp exists
                    if s.query(exists().where(and_(MeterReading.meter_id == meter, MeterReading.recorded_at == recorded_at))).scalar() == True:
                        print("Reading {} - {} already exists".format(meter, recorded_at))
                        # check when existing reading was recorded
                        prev_file_id = s.query(MeterReading.file_id).filter(and_(MeterReading.meter_id == meter, MeterReading.recorded_at == recorded_at)).first()[0]
                        prev_timestamp = s.query(Files.created_at).filter(Files.file_id == prev_file_id).first()[0]
                        print("Existing reading from {} - {}".format(prev_file_id, prev_timestamp))
                        # if existing reading was recorded after this one, do nothing (the existing one is newer so it should be kept)
                        if prev_timestamp > created_at:
                            print("Previous reading is more recent - will not update")
                            pass
                        # if previous reading was recorded before this one, update with the newer reading
                        else:
                            continue

                    df1 = df[(df.meter_id == meter) & (df.date == dates.iloc[i]) & (df.time == times.iloc[i])]
                    new_reading = MeterReading(meter_id=meter, recorded_at=recorded_at, consumption=df1['consumption'].iloc[0], file_id=file_id)
                    s.add(new_reading)
                    s.commit()
                    print("Added reading {} - {}".format(meter, recorded_at))

        else:
            print("File has been read before - data will not be ingested again")

    else:
        print("File {} failed validation - will not be added to database".format(ntpath.basename(filename)))

s.close()
