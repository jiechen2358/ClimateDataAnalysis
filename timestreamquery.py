from _typeshed import Self
import boto3
import json
from botocore.config import Config
import time
import random
import sys, traceback
from timeit import default_timer as timer
import numpy as np
import datetime
import pandas as pd
import os
from collections import defaultdict, namedtuple
import argparse

class ClimateTimeStreamClient:

    db_name = ''
    table_name = ''
    client = None

    def __init__(self, region, db_name, table_name, profile = None):
        self.client = self.createQueryClient(region, profile)
        self.db_name = db_name
        self.table_name = table_name

    '''
    ## Create a timestream query client.
    '''
    def createQueryClient(self, region, profile = None):
        if profile == None:
            print("Using credentials from the environment")

        print(region)
        config = Config()
        if profile != None:
            session = boto3.Session(profile_name = profile)
            client = session.client(service_name = 'timestream-query',
                                    region_name = region, config = config)
        else:
            session = boto3.Session()
            client = session.client(service_name = 'timestream-query',
                                    region_name = region, config = config)

        return client

    def parseDatum(self, c_type, data):
        if ('ScalarType' in c_type):
            return self.parseScalar(c_type['ScalarType'], data.get('ScalarValue'))
        elif ('ArrayColumnInfo' in c_type):
            return self.parseArrayData(c_type['ArrayColumnInfo'], data.get('ArrayValue'))
        elif ('TimeSeriesMeasureValueColumnInfo' in c_type):
            return self.parseTSData(c_type['TimeSeriesMeasureValueColumnInfo'], data.get('TimeSeriesValue'))
        elif ('RowColumnInfo' in c_type):
            return self.parseRowData(c_type['RowColumnInfo'], data.get('RowValue'))
        else:
            raise Exception("All the data is Null???")

    def parseScalar(self, c_type, data):
        if data == None:
            return None
        if (c_type == "VARCHAR"):
            return data
        elif (c_type == "BIGINT"):
            return int(data)
        elif (c_type == "DOUBLE"):
            return float(data)
        elif (c_type == "INTEGER"):
            return int(data)
        elif (c_type == "BOOLEAN"):
            return bool(data)
        elif (c_type == "TIMESTAMP"):
            return data
        else:
            return data

    def parseArrayData(self, c_type, data):
        if data == None:
            return None
        datum_list = []
        for elem in data:
            datum_list.append(self.parseDatum(c_type['Type'], elem))
        return datum_list

    def parseTSData(self, c_type, data):
        if data == None:
            return None
        datum_list = []
        for elem in data:
            ts_data = {}
            ts_data['time'] = elem['Time']
            ts_data['value'] = self.parseDatum(c_type['Type'], elem['Value'])
            datum_list.append(ts_data)
        return datum_list

    def parseRowData(self, c_types, data):
        if data == None:
            return None
        datum_dict = {}
        for c_type, elem in zip(c_types, data['Data']):
            datum_dict[c_type['Name']] = self. parseDatum(c_type['Type'], elem)
        return datum_dict

    def flatModelToDataframe(self, items):
        """
        Translate a Timestream query SDK result into a Pandas dataframe.
        """
        return_val = defaultdict(list)
        for obj in items:
            for row in obj.get('Rows'):
                for c_info, data in zip(obj['ColumnInfo'], row['Data']):
                    c_name = c_info['Name']
                    c_type = c_info['Type']
                    return_val[c_name].append(self.parseDatum(c_type, data))

        df = pd.DataFrame(return_val)
        return df

    ## Execute the passed query using the client and return the result
    ## as a dataframe.
    def executeQueryAndReturnAsDataframe(self, query, timing = False, logFile = None):
        return self.flatModelToDataframe(self.executeQuery(query, timing, logFile))


    ## Executed the passed query using the specified client.
    ## logFile is a file handle which if initialized is assumed to be a valid file handle
    ## where messages will be written. The file handle is expected to have been opened
    ## by the caller. This function does not close the handle and passes it back to the caller.
    def executeQuery(self, query, timing = False, logFile = None):
        try:
            pages = None
            queryId = None
            firstResult = None
            start = timer()
            ## Create the paginator to paginate through the results.
            paginator = self.client.get_paginator('query')
            pageIterator = paginator.paginate(QueryString=query)
            emptyPages = 0
            pages = list()
            lastPage = None
            for page in pageIterator:
                if 'QueryId' in page and queryId == None:
                    queryId = page['QueryId']
                    print("QueryId: {}".format(queryId))

                lastPage = page

                if 'Rows' not in page or len(page['Rows']) == 0:
                    ## We got an empty page.
                    emptyPages +=1
                else:
                    pages.append(page)
                    if firstResult == None:
                        ## Note the time when the first row of result was received.
                        firstResult = timer()

            ## If there were no result, then return the last empty page to carry over the query results context
            if len(pages) == 0 and lastPage != None:
                pages.append(lastPage)
            return pages
        except Exception as e:
            if queryId != None:
                ## Try canceling the query if it is still running
                print("Attempting to cancel query: {}".format(queryId))
                try:
                    self.client.cancel_query(query_id=queryId)
                except:
                    pass
            print(e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)
            if e.response != None:
                queryId = None
                print("RequestId: {}".format(e.response['ResponseMetadata']['RequestId']))
                if 'QueryId' in e.response:
                    queryId = e.response['QueryId']
                print("QueryId: {}".format(queryId))
            raise e
        except KeyboardInterrupt:
            if queryId != None:
                ## Try canceling the query if it is still running
                print("Attempting to cancel query: {}".format(queryId))
                try:
                    self.client.cancel_query(query_id=queryId)
                except:
                    pass
            raise
        finally:
            end = timer()
            if timing == True:
                now = datetime.datetime.utcnow()
                if firstResult != None:
                    timeToFirstResult = firstResult - start
                    timeToReadResults = end - firstResult
                else:
                    timeToFirstResult = end - start
                    timeToReadResults = 0

                timingMsg = "{}. QueryId: {} Time: {}. First result: {}. Time to read results: {}.".format(now.strftime("%Y-%m-%d %H:%M:%S"),
                                                                                                        queryId, round(end - start, 3), round(timeToFirstResult, 3), round(timeToReadResults, 3))
                print(timingMsg)
                if logFile != None:
                    logFile.write("{}\n".format(timingMsg))

    def queryTimeSeries(self, measure_name, city, start, end):
        query = """
        SELECT time,
               City AS city,
               Lat,
               Lon,
               measure_name,
               measure_value::double AS measure
        FROM {0}.{1} 
        WHERE measure_name = '{2}' 
            AND City = '{3}'
            AND time > ago({4})
            AND time <= ago({5})
            
        ORDER BY time ASC
        """.format(self.db_name, self.table_name, measure_name, city, start, end )
        
        results = self.executeQueryAndReturnAsDataframe(query)
        results['measure'] = pd.to_numeric(results['measure'])
        results['time'] = pd.to_datetime(results['time']).dt.date
        
        return results

if __name__ == "__main__":
    endpoint = "us-east-1" # <--- specify the region service endpoint
    profile = "default" # <--- specify the AWS credentials profile
    db_name = "testdb" # <--- specify the database created in Amazon Timestream
    table_name = "climatetable" # <--- specify the table created in Amazon Timestream

    tsClient = ClimateTimeStreamClient(endpoint, db_name, table_name, profile)

    results = tsClient.queryTimeSeries("max_temp", 'Sydney', '15d'  '0d')
    print(results)