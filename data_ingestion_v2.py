import boto3
import time
import numpy
import netCDF4
import pandas as pd
import sys
from botocore.config import Config

ts_dbname = 'testdb'
ts_tablename = 'silo_table'


# todo: add more cities in australia for analysis and visualization
city_config = [
    {"city":"Melbourne","lat":-37.8,"lon": 144.96},
    {"city":"Sydney","lat":-33.85,"lon": 151.2}
    {"city":"Brisbane","lat":-27.45,"lon": 153}
    {"city":"Perth","lat":-31.95,"lon": 115.85}
    {"city":"Adelaide","lat":-34.9,"lon": 138.6}
]

targetLat = -33.85
targetLon = 151.2

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

def fetch_data(raw_records, lat, lon, targetLat, targetLon, dt_range, len_time):
    idxlon = numpy.where(lon==targetLon )
    #print(idxlon[0][0])
    idxlat = numpy.where(lat==targetLat )
    #print(idxlat[0][0])
    
    records = []
    for time_index in range(len_time):
        if time_index< len_time - 20:
            continue

        record = {
            'MeasureValue' : str(round(raw_records[time_index, idxlat, idxlon][0][0],3)),
            'Time': milli_time(dt_range[time_index])
        }
        records.append(record)
    return records

# Todo: The max_temp, daily_rain, etc can be configable. It should has very limited changes
if __name__ == '__main__':
    session = boto3.Session()
    write_client = session.client('timestream-write')
    with netCDF4.Dataset('2021.max_temp.nc', 'r') as dataset:
        lon = numpy.array(dataset.variables['lon'][:])
        lat = numpy.array(dataset.variables['lat'][:])
        starting_dt = dataset.variables['time'].units[11:22]
        end_dt = dataset.variables['time'].units[11:15]+'-12-31'
        dt_range_perfile = pd.date_range(start = starting_dt, end = end_dt)
        len_time = numpy.array(dataset.variables['time'][:]).shape[0]

        data = dataset.variables['max_temp'][:]
        raw_records = numpy.array(data)
        raw_records = numpy.where(raw_records<0, 0, raw_records)

        dimensions = [
        {'Name': 'Lat', 'Value': str(targetLat)},
        {'Name': 'Lon', 'Value': str(targetLon)}
        ]
        common_attributes = {
            'Dimensions': dimensions,
            'MeasureName': 'MaxTemp',
        }
        records =  fetch_data(raw_records, lat, lon, targetLat, targetLon, dt_range_perfile, len_time)
        #print(records)
        upload_to_timestream(write_client, ts_dbname, ts_tablename, records, common_attributes)
        print("done")