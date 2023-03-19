import csv
from sqlalchemy import create_engine, MetaData, Integer, String, Table, Column, text

engine = create_engine('sqlite:///measurements.db')
metadata = MetaData()

stations = Table('stations', metadata,
                 Column('id', Integer),
                 Column('station', String),
                 Column('latitude', String),
                 Column('longitude', String),
                 Column('elevation', String),
                 Column('name', String),
                 Column('country', String),
                 Column('state', String),
                 )

measurements = Table('measurements', metadata,
                     Column('id', Integer),
                     Column('station', String),
                     Column('date', String),
                     Column('precip', String),
                     Column('tobs', String)
                     )

metadata.create_all(engine)

conn = engine.connect()

with open("clean_stations.csv") as f:
    reader = csv.reader(f)
    next(reader)
    for index, row in enumerate(reader):
        conn.execute(stations.insert().values(
            id=index+1, station=row[0], latitude=row[1], longitude=row[2], elevation=row[3], name=row[4], country=row[5], state=row[6]))

with open("clean_measure.csv") as f:
    reader = csv.reader(f)
    next(reader)
    for index, row in enumerate(reader):
        conn.execute(measurements.insert().values(
            id=index+1, station=row[0], date=row[1], precip=row[2], tobs=row[3]))


requested = conn.execute(text("SELECT * FROM stations LIMIT 5")).fetchall()
print(requested)
conn.close()
