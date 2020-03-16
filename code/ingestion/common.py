# coding=utf-8
#  Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

# Constants and anything else that would otherwise need to be duplicated between files

# std lib
from typing import List, Dict, Tuple, Mapping, IO, Any, Union
from os import path, getenv, environ
from pathlib import Path
from csv import reader
from configparser import ConfigParser

# boto3
from boto3.session import Session

# pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, DataType, ByteType, ShortType, IntegerType, FloatType, \
    DoubleType, StringType, BinaryType, BooleanType, TimestampType, DateType


# === Constants ===

TRIPFILE_METADATA_COLS = [['Key', StringType],
                          ['Filename', StringType],
                          ['Type', StringType],
                          ['Year', ShortType],
                          ['Month', ByteType]]

# 6 or 7 decimal places of Decimal Degrees equates to about 100 mm at the equator. more than enough for our application
SPARK_DATATYPE_FOR_LATLONG = FloatType

# taking an inclusion list approach to filtering
DESIRED_COLUMNS: List[Tuple[Tuple[str, DataType], List[str]]] = [
    (('Pickup_DateTime', TimestampType),
     ['PICKUP_DATETIME', 'TRIP_PICKUP_DATETIME', 'LPEP_PICKUP_DATETIME', 'TPEP_PICKUP_DATETIME']),
    (('Dropoff_DateTime', TimestampType),
     ['DROPOFF_DATETIME', 'TRIP_DROPOFF_DATETIME', 'LPEP_DROPOFF_DATETIME', 'TPEP_DROPOFF_DATETIME']),
    (('Passenger_Count', ByteType), ['PASSENGER_COUNT']),
    (('Pickup_Longitude', SPARK_DATATYPE_FOR_LATLONG), ['START_LON', 'PICKUP_LONGITUDE']),
    (('Dropoff_Longitude', SPARK_DATATYPE_FOR_LATLONG), ['END_LON', 'DROPOFF_LONGITUDE']),
    (('Pickup_Latitude', SPARK_DATATYPE_FOR_LATLONG), ['START_LAT', 'PICKUP_LATITUDE']),
    (('Dropoff_Latitude', SPARK_DATATYPE_FOR_LATLONG), ['END_LAT', 'DROPOFF_LATITUDE'])
]
# to be added with implementation of join:
# (('Pickup_TZ_ID', ShortType), ['PULocationID']),
# (('Dropoff_TZ_ID', ShortType), ['DOLocationID'])

# save to an intermediate file set for now, working around geomesa_pyspark issues
# NOTE: as things currently stand, using geomesa_pyspark rather than intermediate files would also lead to actual 
# timestamps (rather than week-wrapped ones) being used in the feature IDs
USE_INTERMEDIATE_FILE = True
INTERMEDIATE_USE_S3 = False
# NOTE: the code assumes the file extension is the same as the label used here
INTERMEDIATE_FORMAT = 'avro'
# list of components of the path within which intermediate file sets should be saved, whether in S3 bucket or HDFS 
INTERMEDIATE_DIRS = ['intermediate']
# unfortunately, for avro, geomesa ingest doesn't support snappy, and spark doesn't support gzip
# their only overlap: uncompressed, bzip2, and xz
# also, spark uses compression within the file, while geomesa seems to require compression of the whole file (or merely
# an unconventional suffixing of the files as if they were compressed whole)
INTERMEDIATE_COMPRESSION = 'uncompressed'


# === Semi-constants ===
# Things deterministically derived from constants but not technically constant (or dependent on things that aren't)

# generate dict mapping each old_name to its (new_name, data_type) tuple (without causing redundant copies thereof)
attributes_for_col: Dict[str, Tuple[str, DataType]] = {}
for tup, old_names in DESIRED_COLUMNS:
    attributes_for_col.update([(old_name, tup) for old_name in old_names])

# pre-make set of keys of attributes_for_col (i.e. the old_names) so that it's not re-constructed from the dict each
# time build_structfield_for_column is called
desired_column_initial_names = attributes_for_col.keys()


# === Functions ===

def load_config() -> Mapping[str, Mapping[str, str]]:
    """
    Load configuration information from predictrip-defaults.ini and any predictrip-site.ini

    :return: a ConfigParser
    """
    repo_root = Path(__file__).parent.parent.parent

    predictrip_config_dir = path.join(repo_root, 'config', 'predictrip')

    config = ConfigParser(dict_type=dict, empty_lines_in_values=False)
    config.read_file(open(path.join(predictrip_config_dir, 'predictrip-defaults.ini')))
    config.read(path.join(predictrip_config_dir, 'predictrip-site.ini'))

    # TODO: if not specified in predictrip-site.ini, check AWS credential sources in order checked by aws jars and boto
    #  (see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#configuring-credentials),
    #  then add whatever options to spark_conf-generation needed to distribute the first one present to workers. pass
    #  credentials as params to boto's client method

    # TODO: to avoid configuration duplication, get from other packages' config files and remove from predictrip files:
    #  config/hadoop/core-site.xml: name_node_host and name_node_port

    return config


def get_boto_session(config: Mapping[str, Mapping[str, str]]) -> Session:
    """
    Build a boto3 session configured for predictrip

    :type config: Mapping of predictrip configuration items
    :return: boto session instance
    """
    return Session(aws_access_key_id=config['AWS']['access_key_id'],
                   aws_secret_access_key=config['AWS']['secret_access_key'])


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


def get_s3_bucket(s3_resource, config: Mapping[str, Mapping[str, str]]):
    """
    Get a boto3 S3 Bucket instance providing access to the bucket we're interested in
    :type s3_resource: boto S3 resource object
    :type config: mapping of predictrip configuration items
    :return: S3 Bucket instance
    """
    # TODO: figure out best type hints for return and s3_resource input
    # TODO: try, raise own error if unable to read bucket? (more helpful message?)
    return s3_resource.Bucket(config['AWS']['s3_bucket_name'])


def build_structtype_for_file(file: IO, verify_eol=False) -> StructType:
    """
    Given a file-like object containing CSV-formatted data, return a StructType object representing its schema.
    Optionally, verify that the header row appears to be complete.

    :param file: File-like object containing bytes of CSV-formatted text
    :param verify_eol: whether to verify that the end of the header row was reached. Useful if file is a stub.
    :return: pyspark.sql.types.StructType (list-like collection of pyspark.sql.types.StructField objects) describing the
    schema of file
    """

    # TODO: get this working using an Iterator. i.e. without reading whole file to pass to csv.reader
    csv_rows = reader(file.read().decode().split("\r\n", 2))
    try:
        column_names = csv_rows.__next__()
    except Exception:
        raise Exception('CSV file seems to be empty')
    # ensure we got the full header by verifying detection of at least one more row
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
    Build a pyspark.sql.types.StructField instance for a trip data CSV column bearing the name given. If it's among the
    columns that will be retained (DESIRED_COLUMNS), the name in the StructField will reflect any desired changes. If
    it's not, its name won't be changed and Spark will be told it's a string to avoid unnecessary parsing and type
    conversion.

    :param column_name: name of column as it appears in trip data CSV header
    :return: pyspark.sql.types.StructField describing the column for PySpark
    """

    # Note: if this function starts being called from code running on Spark workers, it might be better to create the
    # set of keys in the caller (or its caller, etc) rather than at the module level, so that it doesn't need to be
    # serialized to the workers. Trade-off between unnecessary re-computation of unchanging result within each executor
    # and unnecessary transfer from the driver of an object that can be easily derived from other transferred data.
    col_name_standardized = column_name.strip().upper()
    if col_name_standardized in desired_column_initial_names:
        (final_name, data_type) = attributes_for_col[col_name_standardized]
    else:
        final_name = column_name
        data_type = StringType
    # note: the () after data_type is needed to actually instantiate the class
    return StructField(final_name, data_type())


if __name__ == '__main__':
    raise Exception('This file is only meant to be imported, not executed')
