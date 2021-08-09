from netCDF4 import Dataset
import glob
import numpy
import pandas as pd

def ncdump(nc_fid, verb=True):
    '''
    ncdump outputs dimensions, variables and their attribute information.
    The information is similar to that of NCAR's ncdump utility.
    ncdump requires a valid instance of Dataset.

    Parameters
    ----------
    nc_fid : netCDF4.Dataset
        A netCDF4 dateset object
    verb : Boolean
        whether or not nc_attrs, nc_dims, and nc_vars are printed

    Returns
    -------
    nc_attrs : list
        A Python list of the NetCDF file global attributes
    nc_dims : list
        A Python list of the NetCDF file dimensions
    nc_vars : list
        A Python list of the NetCDF file variables
    '''
    def print_ncattr(key):
        """
        Prints the NetCDF file attributes for a given key

        Parameters
        ----------
        key : unicode
            a valid netCDF4.Dataset.variables key
        """
        try:
            print("\t\ttype:", repr(nc_fid.variables[key].dtype))
            for ncattr in nc_fid.variables[key].ncattrs():
                print('\t\t%s:' % ncattr,\
                      repr(nc_fid.variables[key].getncattr(ncattr)))
        except KeyError:
            print("\t\tWARNING: %s does not contain variable attributes" % key)

    # NetCDF global attributes
    nc_attrs = nc_fid.ncattrs()
    if verb:
        print ("NetCDF Global Attributes:")
        for nc_attr in nc_attrs:
            print('\t%s:' % nc_attr, repr(nc_fid.getncattr(nc_attr)))
    nc_dims = [dim for dim in nc_fid.dimensions]  # list of nc dimensions
    # Dimension shape information.
    if verb:
        print("NetCDF dimension information:")
        for dim in nc_dims:
            print("\tName:", dim)
            print("\t\tsize:", len(nc_fid.dimensions[dim]))
            print_ncattr(dim)
    # Variable information.
    nc_vars = [var for var in nc_fid.variables]  # list of nc variables
    if verb:
        print("NetCDF variable information:")
        for var in nc_vars:
            if var not in nc_dims:
                print('\tName:', var)
                print("\t\tdimensions:", nc_fid.variables[var].dimensions)
                print("\t\tsize:", nc_fid.variables[var].size)
                print_ncattr(var)
    return nc_attrs, nc_dims, nc_vars

idx=0

def ncwrite (ncfile, latRange, longitRange):

    print("Read contents")
    for dim in ncfile.dimensions.items():
        print(dim)

    lon = numpy.array(ncfile.variables['lon'][:])
    lat = numpy.array(ncfile.variables['lat'][:])
    time = numpy.array(ncfile.variables['time'][:])
    #print(ncfile.variables['lon'])
    #print(ncfile.variables['lat'])
    #print(ncfile.variables['time'])
    dailyrain = numpy.array(ncfile.variables['daily_rain'][:],dtype=type(ncfile.variables))


    starting_dt = ncfile.variables['time'].units[11:22]
    end_dt = ncfile.variables['time'].units[11:15]+'-12-31'

    dt_range_perfile = pd.date_range(start = starting_dt, end = end_dt)

    #print(dt_range_perfile)

    #df_range = numpy.arange(0,ncfile.variables['lon'].size*ncfile.variables['lat'].size*ncfile.variables['time'].size)
    df_range = numpy.arange(0,11*11*ncfile.variables['time'].size)

    #df2 = pd.DataFrame(0,columns = ['Time','Latitude','Longitude','MeasureName','MeasureValue'],index = df_range)

    dt = numpy.arange(0,ncfile.variables['time'].size)
    df = pd.DataFrame(0,columns = ['Time','Latitude','Longitude','MeasureName','MeasureValue'],index = dt_range_perfile)

    lt_rng = numpy.arange(0,ncfile.variables['lat'].size)
    ln_rng = numpy.arange(0,ncfile.variables['lon'].size)

    dict_row={}
    rows_list = []
    for time_index in dt:
        for lt in lt_rng:
            for ln in ln_rng:
                print("Precipitation for:", dt_range_perfile[time_index])
                #print(lat[lt],lon[ln],dailyrain[time_index,lt,ln])
                dict_row = {'Time': str(dt_range_perfile[time_index]), 'Latitude' : lat[lt], 'Longitude' : lon[ln], 'MeasureName': 'DailyRain', 'MeasureValue' : dailyrain[time_index][lt][ln]}
                #rows_list.append(dict_row)
                producer.send("ClimateDataAnalysis", dict_row)

    #df2 = pd.DataFrame(rows_list)
    #df2.select(to_json(struct($"Time",$"Latitude",$"Longitude",$"MeasureName",$"MeasureValue")).alias("value"))
    #      .write
    #      .format("kafka")
    #      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    #      .option("topic", "ClimateDataAnalysis")
    #      .save()
   #df2.to_csv("precipitation_2020_data.csv")

import sys
import os
import fileinput
import boto3
import codecs
import json

from kafka import KafkaProducer
from kafka.errors import KafkaError
os.environ["S3_NC_CONFIG"] = "/home/ec2-user/.s3nc.json"
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset

producer = KafkaProducer(
    bootstrap_servers=['b-1.cccresearchprojectclu.q4ua60.c12.kafka.us-east-1.amazonaws.com:9092','b-2.cccresearchprojectclu.q4ua60.c12.kafka.us-east-1.amazonaws.com:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

yearType = sys.argv[1]
longitRange = 0 #sys.argv[2]
latRange = 0 #sys.argv[3]

s3 = boto3.resource('s3')

my_bucket = s3.Bucket('s3://silo-open-data/annual/daily_rain/')

ncfiles = my_bucket.objects.all()
print(ncfiles)

#for ifile in range(len(ncfiles)):
#    if (yearType in ncfiles[ifile]):
#        sfile = Dataset('s3://silo-open-data/annual/daily_rain/'+ncfiles[ifile],mode='r', format='NETCDF4')
s3_ds = Dataset('s3://silo-open-data/annual/daily_rain/'+yearType+'.daily_rain.nc', 'r', format='NETCDF4')
ncwrite(s3_ds, latRange, longitRange )

