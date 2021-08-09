# Climate data analysis and visualization on Amazon Timestream


## Data Ingestion

Please check the code in https://github.com/jiechen2358/ClimateDataAnalysis/blob/main/data_ingestion_v2.py



- Config the destination of Timestream DB and Table

```
ts_dbname = 'testdb'
ts_tablename = 'climatetable'
```

- Allow to upload multi data sources.

```
source_configs = [
    {"year":"2020", "variable":"max_temp"},
    {"year":"2021", "variable":"max_temp"},
    {"year":"2020", "variable":"min_temp"},
    {"year":"2021", "variable":"min_temp"},
    {"year":"2020", "variable":"daily_rain"},
    {"year":"2021", "variable":"daily_rain"},
    {"year":"2020", "variable":"et_morton_actual"},
    {"year":"2021", "variable":"et_morton_actual"},
    {"year":"2020", "variable":"et_morton_potential"},
    {"year":"2021", "variable":"et_morton_potential"},
    {"year":"2020", "variable":" et_morton_wet"},
    {"year":"2021", "variable":" et_morton_wet"},
    # + more measurements
]
```

- Flexible to add more cities or other custom configs

```
city_configs = [
        {"city":"Melbourne", "lat":-37.8,"lon": 144.95},
        {"city":"Sydney", "lat":-33.85,"lon": 151.2},
        {"city":"Brisbane", "lat":-27.45,"lon": 153},
        {"city":"Perth", "lat":-31.95,"lon": 115.85},
        {"city":"Adelaide", "lat":-34.9,"lon": 138.6},
        {"city":"Gold Coast", "lat":-28.0,"lon": 153.40},
        {"city":"NewCastle", "lat":-32.9,"lon": 151.75},
        {"city":"Canberra", "lat":-35.30,"lon": 149.10},
        {"city":"Sunshine Coast", "lat":-26.65,"lon": 153.05},
        {"city":"Central Coast", "lat":-33.30,"lon": 151.20},
        # + more locations.
    ]
```


## Data Consumption

We have implemented the SDK of **ClimateTimeStreamClient**, please check more details here: https://github.com/jiechen2358/ClimateDataAnalysis/blob/main/timestreamquery.py

Our  data  ingestion  has  empowered  complicated  data  withdiverse measurements on multi locations and long time ranges.We  developed  a  Python  SDK

1. **Query  Client**  It’s  using  the  boto3  to  connect  with  theAmazon  Timestream  specifying  the  region  and  configu-rations.

2. **Query  Builder**  The  query  builder  takes  the  parameterof   measure   name,   city,   start   time,   and   end   time   toautomatically build the SQL query.

3. **Query Execution** The query execution will leverage theclient  to  call  the  Amazon  Timestream  and  run  the  builtquery to fetch the results. To enable more comprehensiveanalysis,  the  results  will  be  converted  as  pandas  dataframe.

Here's the example to call the SDK to query data via the easy-to-use interface:


```
if __name__ == "__main__":
    endpoint = "us-east-1" # <--- specify the region service endpoint
    profile = "default" # <--- specify the AWS credentials profile
    db_name = "testdb" # <--- specify the database created in Amazon Timestream
    table_name = "climatetable" # <--- specify the table created in Amazon Timestream

    tsClient = ClimateTimeStreamClient(endpoint, db_name, table_name, profile)

    results = tsClient.queryTimeSeries("max_temp", 'Sydney', '15d'  '0d')
    print(results)
```

## SageMaker Notebook

**Setup**

To Setup the SageMaker and Notebook:

1. Set  up  Amazon  SageMaker  with  AWS  account  and  on-board to Amazon SageMaker Studio.
2. Set up Amazon S3 Bucket for Amazon SageMaker as theanalysis and ML storage
3. Create  Notebook  instance  with  boto3  and  other  corelibraries installed.
4. Enable   the   IAM   services   to   access   the   AmazonTimeStream and S3 in SageMaker notebook

More details can be found here: 

https://docs.aws.amazon.com/timestream/latest/developerguide/Sagemaker.html

**Notebook**

We have developed one DEMO notebook on SageMaker to easily consume the data via the above SDK and performt the analysis and ML modeling.
Please check the example:
https://github.com/jiechen2358/ClimateDataAnalysis/blob/main/Timestream_SageMaker_Demo.ipynb



## Other Tech Setup and Configs

sudo yum update

sudo yum install git

sudo yum install python3-devel

sudo python3 -m pip install -e git+https://github.com/cedadev/S3-netcdf-python.git#egg=S3netCDF4

Create a configuration file in user home directory

cat .s3nc.json

{

    "version": "9",
    "hosts": {
        "s3://silo-open-data": {
            "alias": "silo-open-data",
                "url": "https://s3-ap-southeast-2.amazonaws.com/silo-open-data",
                "credentials": {
                    "accessKey": "<your access id>",
                    "secretKey": "<your secret key>"
                },
                "backend": "s3aioFileObject",
                "api": "S3v4"
        }
    },
    "backends": {
        "s3aioFileObject" : {
            "maximum_part_size": "50MB",
            "maximum_parts": 8,
            "enable_multipart_download": true,
            "enable_multipart_upload": true,
            "connect_timeout": 30.0,
            "read_timeout": 30.0
        },
        "s3FileObject" : {
            "maximum_part_size": "50MB",
            "maximum_parts": 4,
            "enable_multipart_download": false,
            "enable_multipart_upload": false,
            "connect_timeout": 30.0,
            "read_timeout": 30.0
        }
    },
    "cache_location": "/cache_location/.cache",
    "resource_allocation" : {
        "memory": "1GB",
        "filehandles": 20
    }
}

To execute:

python3 climate_data_extract_singleFile.py 2020
