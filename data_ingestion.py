from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, Time, DateTime
from sqlalchemy.orm import sessionmaker

# vars
DATABASE_URI = 'postgres+psycopg2://test_user:test_user@localhost:5432/gazprom_data_exercise'
Base = declarative_base()

class MeterReading(Base):
    __tablename__ = 'smart_meter_data'
    id = Column(Integer, primary_key=True)
    meter_id = Column(String)
    date = Column(String)
    time = Column(String)
    consumption = Column(Float)
    file_id = Column(String)

    def __repr__(self):
        return "<MeterReading(meter_id='{}', date='{}', time={}, consumption={}, file_id={})>"\
                .format(self.meter_id, self.date, self.time, self.consumption, self.file_id)

class Meters(Base):
    __tablename__ = 'all_meters'
    id = Column(Integer, primary_key=True)
    meter_id = Column(String)

    def __repr__(self):
        return "<MeterReading(meter_id='{}')>".format(self.meter_id)

class Files(Base):
    __tablename__ = 'all_files'
    id = Column(Integer, primary_key=True)
    file_id = Column(String)
    created_at = Column(DateTime)

    def __repr__(self):
        return "<MeterReading(file_id='{}')>".format(self.file_id)

engine = create_engine(DATABASE_URI)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
s = Session()



s.close()
