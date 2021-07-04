import boto3
import time
import numpy
import netCDF4
import pandas as pd
import sys
from botocore.config import Config

ts_dbname = 'testdb'
ts_tablename = 'climatetable'

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

if __name__ == '__main__':
    session = boto3.Session()
    write_client = session.client('timestream-write')
    with netCDF4.Dataset('2021.daily_rain.nc', 'r') as dataset:
        lon = numpy.array(dataset.variables['lon'][:])
        lat = numpy.array(dataset.variables['lat'][:])
        starting_dt = dataset.variables['time'].units[11:22]
        end_dt = dataset.variables['time'].units[11:15]+'-12-31'
        dt_range_perfile = pd.date_range(start = starting_dt, end = end_dt)
        len_time = numpy.array(dataset.variables['time'][:]).shape[0]

        data = dataset.variables['daily_rain'][:]
        dailyrain = numpy.array(data)
        dailyrain = numpy.where(dailyrain<0, 0, dailyrain)

        idxlon = numpy.where( lon==targetLon )
        idxlat = numpy.where( lat==targetLat )
        
        dimensions = [
                    {'Name': 'Lat', 'Value': str(targetLat)},
                    {'Name': 'Lon', 'Value': str(targetLon)}
                ]
        common_attributes = {
            'Dimensions': dimensions,
            'MeasureName': 'DailyRain',
            'MeasureValueType':'DOUBLE',
        }

        records = []
        for time_index in range(len_time):
            if time_index< len_time - 100:
                continue
            
            record = {
                    'MeasureValue' : str(round(dailyrain[time_index, idxlat, idxlon][0][0],3)),
                    'Time': milli_time(dt_range_perfile[time_index])
                    #'Time':current_milli_time()
            }
            records.append(record)

        print(records)
        try:
            result = write_client.write_records(DatabaseName=ts_dbname, 
                                                TableName=ts_tablename,
                                                Records=records,
                                                CommonAttributes=common_attributes)
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except write_client.exceptions.RejectedRecordsException as err:
            print_rejected_records_exceptions(err)