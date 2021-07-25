# ClimateDataAnalysis

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

python3 climate_data_processing_producer.py 2020
