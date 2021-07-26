import boto3
import time
import numpy
import netCDF4
import pandas as pd
import sys
from botocore.config import Config

# Config the destination of Timestream DB and Table
ts_dbname = 'testdb'
ts_tablename = 'climatetable'

#Allow to upload multi data sources.
source_configs = [
    {"year":"2020", "variable":"max_temp"},
    {"year":"2021", "variable":"max_temp"},
    {"year":"2020", "variable":"min_temp"},
    {"year":"2021", "variable":"min_temp"},
    {"year":"2020", "variable":"daily_rain"},
    {"year":"2021", "variable":"daily_rain"},
]

# Flexible to add more cities or other custom configs
city_configs = [
        {"city":"Melbourne", "lat":-37.8,"lon": 144.95},
        {"city":"Sydney", "lat":-33.85,"lon": 151.2},
        {"city":"Brisbane", "lat":-27.45,"lon": 153},
        {"city":"Perth", "lat":-31.95,"lon": 115.85},
        {"city":"Adelaide", "lat":-34.9,"lon": 138.6},
    ]

def current_milli_time():
    return str(int(round(time.time() * 1000)))

def milli_time(dstime):
    return str(int(dstime.timestamp() * 1000))

def print_rejected_records_exceptions(err):
    print("RejectedRecords: ", err)
    for rr in err.response["RejectedRecords"]:
        print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        if "ExistingVersion" in rr:
            print("Rejected record existing version: ", rr["ExistingVersion"])


def upload_to_timestream(write_client, dbname, tablename, records, common_attributes):
    try:
        result = write_client.write_records(DatabaseName=dbname, 
                                            TableName=tablename,
                                            Records=records,
                                            CommonAttributes=common_attributes)
                                        
        print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
    except write_client.exceptions.RejectedRecordsException as err:
        print_rejected_records_exceptions(err)

def fetch_record_batches(raw_records, lat, lon, targetLat, targetLon, dt_range, len_time):
    idxlon = numpy.where(lon==targetLon )
    #print(idxlon[0][0])
    idxlat = numpy.where(lat==targetLat )
    #print(idxlat[0][0])
    
    batches = []
    records = []
    for time_index in range(len_time):
        if time_index > 0 and time_index % 55 == 0:
            batches.append(records)
            records = []
        record = {
            'MeasureValue' : str(round(raw_records[time_index, idxlat, idxlon][0][0],3)),
            'Time': milli_time(dt_range[time_index])
        }
        records.append(record)

    if len(records) > 0:
        batches.append(records)
    return batches

def process(write_client, year, measure):
    with netCDF4.Dataset(year + '.' + measure + '.nc', 'r') as dataset:
        lon = numpy.array(dataset.variables['lon'][:])
        lat = numpy.array(dataset.variables['lat'][:])
        starting_dt = dataset.variables['time'].units[11:22]
        end_dt = dataset.variables['time'].units[11:15]+'-12-31'
        dt_range_perfile = pd.date_range(start = starting_dt, end = end_dt)
        len_time = numpy.array(dataset.variables['time'][:]).shape[0]

        data = dataset.variables[measure][:]
        raw_records = numpy.array(data)
        raw_records = numpy.where(raw_records<0, 0, raw_records)

        for item in city_configs:
            # Leverage common attributes for the workload saving.
            dimensions = [
                {'Name': 'Lat', 'Value': str(item["lat"])},
                {'Name': 'Lon', 'Value': str(item["lon"])},
                {'Name': 'City', 'Value': str(item["city"])}
            ]
            common_attributes = {
                'Dimensions': dimensions,
                'MeasureName': measure,
            }
            
            # 50 records will be in single batch to save 5o times ingestion.
            batches =  fetch_record_batches(raw_records, lat, lon, item["lat"], item["lon"], dt_range_perfile, len_time)
            for batch in batches:
                upload_to_timestream(write_client, ts_dbname, ts_tablename, batch, common_attributes)
            print(item["city"])

if __name__ == '__main__':
    session = boto3.Session()
    write_client = session.client('timestream-write')

    for source in source_configs:
        process(write_client, source["year"], source["variable"])