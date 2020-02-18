# coding=utf-8
#  Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

# Constants and anything else that would otherwise need to be duplicated between files

from typing import Dict, Tuple, IO, Any

from os import path, getenv, environ
import socket

import csv

from zlib import compress, decompress
from base64 import b64encode, b64decode

from boto3.session import Session

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, DataType, ByteType, ShortType, IntegerType, FloatType, \
    DoubleType, StringType, BinaryType, BooleanType, TimestampType, DateType

from contextlib import contextmanager


# === Constants ===

# temporary measures for remote connection for debugging
hostname = socket.gethostname()
if hostname == 'geomesa':
    PREDICTRIP_REPO_ROOT = path.join(environ['HOME'], 'predictrip')
else:
    PREDICTRIP_REPO_ROOT = path.join(environ['HOME'], 'code', 'insight', 'predictrip')

TRIPFILE_METADATA_COLS = [['Key', StringType],
                          ['Filename', StringType],
                          ['Type', StringType],
                          ['Year', ShortType],
                          ['Month', ByteType]]
# the following must be kept consistent with the preceding
TRIPFILE_METADATA_KEY_COL_IDX = 0
TRIPFILE_METADATA_FILENAME_COL_IDX = 1

# 6 or 7 decimal places of Decimal Degrees equates to about 100 mm at the equator. more than enough for our application
SPARK_DATATYPE_FOR_LATLONG = FloatType

# TODO: if, in the end, no initial column names that differ in only space-padding or capitalization differ in data
#  type, then just strip and lower-case them all before lookup and store only the remaining unique keys
# TODO ?: to avoid unnecessary data type conversions by workers when reading CSVs, change to StringType all columns that
#  we don't actually need to understand during cleaning/homogenization
# tuple structure: ( new name, subclass of pyspark.sql.types.DataType )
# possible alternative might be in using StructType.fromJson() as exemplified here: https://stackoverflow.com/a/36035641
ATTRIBUTES_FOR_COL: Dict[str, Tuple[str, DataType]] = {
    'vendor_name': ('Vendor_Name', StringType),
    'vendor_id': ('Vendor_Name', StringType),  # yes, "id" is actually a string of chars

    'VendorID': ('Vendor_ID', ByteType),

    'hvfhs_license_num': ('HVFHS_License_Num', ByteType),

    'Dispatching_base_num': ('Dispatching_Base', StringType),  # 6-chars, "B" + 0-padded int
    'dispatching_base_num': ('Dispatching_Base', StringType),

    'Trip_Pickup_DateTime': ('Pickup_DateTime', TimestampType),
    'pickup_datetime': ('Pickup_DateTime', TimestampType),
    # ' pickup_datetime': ('Pickup_DateTime', TimestampType),
    'lpep_pickup_datetime': ('Pickup_DateTime', TimestampType),
    'tpep_pickup_datetime': ('Pickup_DateTime', TimestampType),
    'Pickup_DateTime': ('Pickup_DateTime', TimestampType),

    'Trip_Dropoff_DateTime': ('Dropoff_DateTime', TimestampType),
    'dropoff_datetime': ('Dropoff_DateTime', TimestampType),
    # ' dropoff_datetime': ('Dropoff_DateTime', TimestampType),
    'Lpep_dropoff_datetime': ('Dropoff_DateTime', TimestampType),
    'lpep_dropoff_datetime': ('Dropoff_DateTime', TimestampType),
    'tpep_dropoff_datetime': ('Dropoff_DateTime', TimestampType),
    'Dropoff_dateTime': ('Dropoff_DateTime', TimestampType),

    'Passenger_Count': ('Passenger_Count', ByteType),
    'passenger_count': ('Passenger_Count', ByteType),
    # ' passenger_count': ('Passenger_Count', ByteType),
    'Passenger_count': ('Passenger_Count', ByteType),

    'Trip_Distance': ('Trip_Distance', FloatType),
    'trip_distance': ('Trip_Distance', FloatType),
    # ' trip_distance': ('Trip_Distance', FloatType),
    'Trip_distance': ('Trip_Distance', FloatType),
    # seems to have fixed precision of 2 decimal places, at least in some files

    'Start_Lon': ('Pickup_Longitude', SPARK_DATATYPE_FOR_LATLONG),
    'pickup_longitude': ('Pickup_Longitude', SPARK_DATATYPE_FOR_LATLONG),
    # ' pickup_longitude': ('Pickup_Longitude', SPARK_DATATYPE_FOR_LATLONG),
    'Pickup_longitude': ('Pickup_Longitude', SPARK_DATATYPE_FOR_LATLONG),

    'Start_Lat': ('Pickup_Latitude', SPARK_DATATYPE_FOR_LATLONG),
    'pickup_latitude': ('Pickup_Latitude', SPARK_DATATYPE_FOR_LATLONG),
    # ' pickup_latitude': ('Pickup_Latitude', SPARK_DATATYPE_FOR_LATLONG),
    'Pickup_latitude': ('Pickup_Latitude', SPARK_DATATYPE_FOR_LATLONG),

    'PULocationID': ('Pickup_TZ_ID', ShortType),

    'DOLocationID': ('Dropoff_TZ_ID', ShortType),

    'Rate_Code': ('Rate_Code', ByteType),
    'rate_code': ('Rate_Code', ByteType),
    # ' rate_code': ('Rate_Code', ByteType),
    'RateCodeID': ('Rate_Code', ByteType),
    'RatecodeID': ('Rate_Code', ByteType),

    'store_and_forward': ('Store_and_Forward', BooleanType),  # 0 or 1
    'store_and_fwd_flag': ('Store_and_Forward', BooleanType),  # N or Y
    # ' store_and_fwd_flag': ('Store_and_Forward', BooleanType),  # N or Y
    'Store_and_fwd_flag': ('Store_and_Forward', BooleanType),  # N or Y

    'SR_Flag': ('SR_Flag', BooleanType),  # I've seen 1's and blanks

    'End_Lon': ('Dropoff_Longitude', SPARK_DATATYPE_FOR_LATLONG),
    'dropoff_longitude': ('Dropoff_Longitude', SPARK_DATATYPE_FOR_LATLONG),
    # ' dropoff_longitude': ('Dropoff_Longitude', SPARK_DATATYPE_FOR_LATLONG),
    'Dropoff_longitude': ('Dropoff_Longitude', SPARK_DATATYPE_FOR_LATLONG),

    'End_Lat': ('Dropoff_Latitude', SPARK_DATATYPE_FOR_LATLONG),
    'dropoff_latitude': ('Dropoff_Latitude', SPARK_DATATYPE_FOR_LATLONG),
    # ' dropoff_latitude': ('Dropoff_Latitude', SPARK_DATATYPE_FOR_LATLONG),
    'Dropoff_latitude': ('Dropoff_Latitude', SPARK_DATATYPE_FOR_LATLONG),

    'Trip_type': ('Trip_Type', ByteType),  # unknown format. haven't seen it non-blank
    # 'Trip_type ': ('Trip_Type', ByteType),
    'trip_type': ('Trip_Type', ByteType),  # seen integers

    'Payment_Type': ('Payment_Type', StringType),
    'payment_type': ('Payment_Type', StringType),
    # ' payment_type': ('Payment_Type', StringType),
    'Payment_type': ('Payment_Type', StringType),

    'Fare_Amt': ('Fare_Amount', FloatType),
    'fare_amount': ('Fare_Amount', FloatType),
    # ' fare_amount': ('Fare_Amount', FloatType),
    # 'fare_amount ': ('Fare_Amount', FloatType),
    'Fare_amount': ('Fare_Amount', FloatType),

    'surcharge': ('Surcharge_Amount', FloatType),

    'Extra': ('Extra_Amount', FloatType),  # might be same as surcharge. can't tell
    'extra': ('Extra_Amount', FloatType),  # might be same as surcharge. can't tell

    'mta_tax': ('MTA_Tax', FloatType),
    # ' mta_tax': ('MTA_Tax', FloatType),
    'MTA_tax': ('MTA_Tax', FloatType),

    'Tip_Amt': ('Tip_Amount', FloatType),
    'Tip_amount': ('Tip_Amount', FloatType),
    # ' tip_amount': ('Tip_Amount', FloatType),
    'tip_amount': ('Tip_Amount', FloatType),

    'Tolls_Amt': ('Tolls_Amount', FloatType),
    'tolls_amt': ('Tolls_Amount', FloatType),
    'tolls_amount': ('Tolls_Amount', FloatType),
    # ' tolls_amt': ('Tolls_Amount', FloatType),
    'Tolls_amount': ('Tolls_Amount', FloatType),

    'Ehail_fee': ('EHail_Fee', FloatType),
    'ehail_fee': ('EHail_Fee', FloatType),

    'improvement_surcharge': ('Improvement_Surcharge', FloatType),

    'congestion_surcharge': ('Congestion_Surcharge', FloatType),

    'Total_Amt': ('Total_Amount', FloatType),
    'total_amount': ('Total_Amount', FloatType),
    # ' total_amount': ('Total_Amount', FloatType),
    'Total_amount': ('Total_Amount', FloatType)
}

# TODO ?: somehow derive ATTRIBUTES_FOR_COL and UNDESIRED_COLUMNS from a single structure for increased maintainability
# columns to drop when present in input files (referred to by their new/desired names)
UNDESIRED_COLUMNS = ['Vendor_Name', 'Vendor_ID', 'HVFHS_License_Num', 'Dispatching_Base', 'Trip_Distance', 'Rate_Code',
                     'Store_and_Forward', 'SR_Flag', 'Trip_Type', 'Payment_Type', 'Fare_Amount', 'Surcharge_Amount',
                     'Extra_Amount', 'MTA_Tax', 'Tip_Amount', 'Tolls_Amount', 'EHail_Fee', 'Improvement_Surcharge',
                     'Congestion_Surcharge', 'Total_Amount']

# === Functions ===

@contextmanager
def stdout_redirected_to(out_stream: IO):
    """
    Return a context in which stdout is redirected to a given File-like object
    Usage example:
        with stdout_redirected_to(open('foo.txt')):
            do_stuff_you_want_to_redirect()
        do_things_normally_again()
    Very slight modification of code suggested in https://stackoverflow.com/a/54058723

    :param out_stream: File-like object to which to redirect stdout
    """
    # TODO: pin down the full scope of what out_stream could be and update type hint and docstring accordingly
    # TODO: move to a (more re-usable) utility module
    from sys import stdout
    orig_stdout = stdout
    try:
        stdout = out_stream
        yield
    finally:
        stdout = orig_stdout


def get_s3_client(session: Session):
    """
    Build a boto3 S3 client, providing the low-level access needed for, e.g., downloading specific byte ranges of
    objects

    :type session: boto3 Session for/in which to create client
    :return: S3 client instance
    """
    # TODO: figure out proper return type hint. botocore.client.S3 not working
    return session.client('s3')


def get_s3_resource(session: Session):
    """
    Build a boto3 S3 resource, providing the high-level access that, e.g., spares having to page through results

    :type session: boto3 Session for/in which to create resource
    :return: S3 resource instance
    """
    # TODO: figure out best return type hint. type() says it's a boto3.resource.S3, but can't use that in hint
    return session.resource('s3')


def get_s3_bucket(s3_resource, config: Dict[str, Any]):
    """
    Get a boto3 S3 Bucket instance providing access to the bucket we're interested in
    :type s3_resource: boto S3 resource object
    :type config: dictionary representing predictrip configuration
    :return: S3 Bucket instance
    """
    # TODO: figure out best type hints for return and s3_resource input
    # TODO: try, raise own error if unable to read bucket? (more helpful message?)
    return s3_resource.Bucket(config['s3_bucket_name'])


def compress_string(string: str, debugging=False) -> str:
    """
    Compress a UTF-8 string in a way safe for passage to fork/exec, but not necessarily shells

    :param string: string to be compressed
    :param debugging: whether to print output potentially helpful in debugging
    :return: string of base64-encoded bytes
    """
    # TODO: take zlib compression level as arg
    string_bytes = string.encode('utf-8')
    if debugging:
        print('inputted string is {} bytes in size'.format(len(string_bytes)))
    string_compressed = compress(string_bytes)
    if debugging:
        print('string is {} bytes in size after compression with zlib (default level, 6)'
              .format(len(string_compressed)))
        for n in range(1, 10):
            print('string is {} bytes in size after compression with zlib (level {})'
                  .format(n, len(compress(string_bytes, level=n))))
    string_b64 = b64encode(string_compressed)
    if debugging:
        print('string is {} bytes in size after base64-encoding'.format(len(string_b64)))
    return string_b64.decode('utf-8')


def decompress_string(string: str) -> str:
    """
    Decompress a UTF-8 string compressed by compress_string

    :param string: base64-encoded string to be decompressed
    :return: original string
    """
    # b64 string -> b64 byte array -> compressed byte array
    b64_bytes = b64decode(string.encode('utf-8'))
    # compressed byte array -> byte array -> original string
    string_bytes = decompress(b64_bytes)
    string_decompressed = string_bytes.decode('utf-8')
    return string_decompressed


def build_structtype_for_file(file: IO, verify_eol=False) -> StructType:
    """
    Given a file-like object containing CSV-formatted data, return a StructType object representing its schema.
    Optionally, verify that the header row appears to be complete.

    :param file: File-like object containing bytes of CSV-formatted text
    :param verify_eol: whether to verify that the end of the header row was reached. Useful if file is a stub.
    :return: pyspark.sql.types.StructType (list-like collection of pyspark.sql.types.StructField objects) describing the
    schema of file
    """
    # Note of possible relevance in case of major change in implementation:
    # Name and DataType are required params of StructField constructor. If necessary, we could use instances of
    # pyspark.sql.types.NullType as dummy values, as that's used when types can't be inferred, which is only relevant
    # when inferring. (i.e. that would never be the "right" data type for us.)

    # TODO: get this working using an Iterator. i.e. without reading whole file to pass to csv.reader
    csv_rows = csv.reader(file.read().decode().split("\r\n", 2))
    try:
        column_names = csv_rows.__next__()
    except Exception:
        raise Exception('CSV file seems to be empty')
    # ensure we got the full header by checking for detection of at least one more row
    if verify_eol:
        try:
            csv_rows.__next__()
        except Exception:
            # if a subsequent row wasn't found, we can't know that we downloaded the entirety of the first one.
            # future improvement: make it progressively download additional data until it does get an EOL
            raise Exception('No EOL encountered. Consider increasing header stub size.')
    schema = [build_structfield_for_column(col_name) for col_name in column_names]
    return StructType(schema)


def build_structfield_for_column(column_name: str) -> StructField:
    """
    Build a pyspark.sql.types.StructField instance describing a trip data CSV column bearing the name given. The name in
    the StructField will reflect any desired changes.

    :param column_name: name of column as it appears in trip data CSV header
    :return: pyspark.sql.types.StructField describing the column for PySpark
    """
    try:
        # TODO: assuming no needs arise for differing treatment of col names differing only in case, add .lower() and
        #  remove upper-case dictionary entries accordingly
        attribs = ATTRIBUTES_FOR_COL[column_name.strip()]
    except KeyError:
        raise
    # note: the () after attribs(2) is needed to actually instantiate the class that is attribs(2)
    sf = StructField(attribs[0], attribs[1]())
    return sf


def load_config() -> Dict[str, Any]:
    """
    Load relevant configuration items from various files
    :return: predictrip options
    """
    # TODO: use ChainMap (https://docs.python.org/3.6/library/collections.html#chainmap-objects)
    from configparser import ConfigParser
    parser = ConfigParser()
    parser.read(path.join(PREDICTRIP_REPO_ROOT, 'config', 'predictrip', 'predictrip-site.ini'))
    config = {}

    # TODO: address possibility of sections not existing
    # TODO: store defaults in a predictrip-defaults.ini file rather than here

    # TODO: check for AWS credential sources in order checked by aws jars and boto
    #  (see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#configuring-credentials),
    #  then add whatever options to spark_conf-generation needed to distribute the first one present to workers. pass
    #  credentials as params to boto's client method
    # TODO: otherwise, get either AWS key from ~/.aws/credentials (also using configparser) if not specified in
    #  predictrip-site.ini
    config['aws_access_key'] = parser['AWS'].get('AccessKeyId')
    config['aws_secret_access_key'] = parser['AWS'].get('SecretAccessKey')

    config['s3_bucket_name'] = parser['S3'].get('BucketName', 'nyc-tlc')
    config['s3_trips_prefix'] = parser['S3'].get('TripFilePrefix', 'trip data')
    config['csv_stub_bytes'] = int(parser['S3'].get('CsvHeaderStubSize'))

    config['spark_geomesa_jar'] = parser['Spark'].get('GeoMesaJar')
    # TODO: look for in config/spark/spark-env.sh instead of predictrip config
    config['spark_master_port'] = parser['Spark'].get('MasterPort', 7077)
    config['spark_home'] = parser['Spark'].get('Home', getenv('SPARK_HOME', '/usr/local/spark'))
    # val = parser['Spark'].get('ExecutorCores', None)
    # if val is not None:
    #     config['spark.executor.cores'] = val
    return config


if __name__ == '__main__':
    raise Exception('This file is only meant to be imported, not executed')