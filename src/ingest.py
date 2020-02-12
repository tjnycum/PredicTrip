#!/usr/bin/env python3
# Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

import os
from typing import List, Dict, Union, Tuple
import csv

import boto3
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient

import pandas

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, DataType, ByteType, ShortType, IntegerType, FloatType, \
    DoubleType, StringType, BinaryType, BooleanType, TimestampType, DateType
from pyspark.sql.dataframe import DataFrame

# TODO: make into Ingestor class

# TODO: Get these from spark-env.sh in ../config/
SPARK_MASTER_PORT = 7077  # the default

SPARK_MASTER = 'sparkmaster'
PREDICTRIP_CODE_DIR_ROOT = os.path.join(os.getenv('HOME'), 'predictrip')

STRUCTTYPE_FOR_LATLONG = DoubleType

# TODO: if, in the end, no intitial column names that differ in only space-padding or capitalization differ in data
#  type, then just strip and uniformize case before looking them up and store only the remaining unique keys
# tuple structure: ( new name, subclass of pyspark.sql.types.DataType )
# possible alternative might be in using StructType.fromJson() as exemplified here: https://stackoverflow.com/a/36035641
ATTRIBUTES_FOR_COL: Dict[str, Tuple[str, DataType]] = {
    'vendor_name': ('Vendor_Name', StringType),
    'vendor_id': ('Vendor_Name', StringType),  # yes, "id" is actually a string of chars

    'VendorID': ('Vendor_ID', ByteType),

    'hvfhs_license_num': ('HVFHS_License_Num', ByteType),

    'Dispatching_base_num': ('Dispatching_Base_Num', StringType),  # 6-chars, "B" + 0-padded int
    'dispatching_base_num': ('Dispatching_Base_Num', StringType),

    'Trip_Pickup_DateTime': ('Pickup_Time', TimestampType),
    'pickup_datetime': ('Pickup_Time', TimestampType),
    # ' pickup_datetime': ('Pickup_Time', TimestampType),
    'lpep_pickup_datetime': ('Pickup_Time', TimestampType),
    'tpep_pickup_datetime': ('Pickup_Time', TimestampType),
    'Pickup_DateTime': ('Pickup_Time', TimestampType),

    'Trip_Dropoff_DateTime': ('Dropoff_Time', TimestampType),
    'dropoff_datetime': ('Dropoff_Time', TimestampType),
    # ' dropoff_datetime': ('Dropoff_Time', TimestampType),
    'Lpep_dropoff_datetime': ('Dropoff_Time', TimestampType),
    'lpep_dropoff_datetime': ('Dropoff_Time', TimestampType),
    'tpep_dropoff_datetime': ('Dropoff_Time', TimestampType),
    'Dropoff_dateTime': ('Dropoff_Time', TimestampType),

    'Passenger_Count': ('Passenger_Count', ByteType),
    'passenger_count': ('Passenger_Count', ByteType),
    # ' passenger_count': ('Passenger_Count', ByteType),
    'Passenger_count': ('Passenger_Count', ByteType),

    'Trip_Distance': ('Trip_Distance', FloatType),
    'trip_distance': ('Trip_Distance', FloatType),
    # ' trip_distance': ('Trip_Distance', FloatType),
    'Trip_distance': ('Trip_Distance', FloatType),
    # seems to have fixed precision of 2 decimal places, at least in some files

    'Start_Lon': ('Pickup_Long', STRUCTTYPE_FOR_LATLONG),
    'pickup_longitude': ('Pickup_Long', STRUCTTYPE_FOR_LATLONG),
    # ' pickup_longitude': ('Pickup_Long', STRUCTTYPE_FOR_LATLONG),
    'Pickup_longitude': ('Pickup_Long', STRUCTTYPE_FOR_LATLONG),

    'Start_Lat': ('Pickup_Lat', STRUCTTYPE_FOR_LATLONG),
    'pickup_latitude': ('Pickup_Lat', STRUCTTYPE_FOR_LATLONG),
    # ' pickup_latitude': ('Pickup_Lat', STRUCTTYPE_FOR_LATLONG),
    'Pickup_latitude': ('Pickup_Lat', STRUCTTYPE_FOR_LATLONG),

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

    'End_Lon': ('Dropoff_Long', STRUCTTYPE_FOR_LATLONG),
    'dropoff_longitude': ('Dropoff_Long', STRUCTTYPE_FOR_LATLONG),
    # ' dropoff_longitude': ('Dropoff_Long', STRUCTTYPE_FOR_LATLONG),
    'Dropoff_longitude': ('Dropoff_Long', STRUCTTYPE_FOR_LATLONG),

    'End_Lat': ('End_Lat', STRUCTTYPE_FOR_LATLONG),
    'dropoff_latitude': ('Dropoff_Lat', STRUCTTYPE_FOR_LATLONG),
    # ' dropoff_latitude': ('Dropoff_Lat', STRUCTTYPE_FOR_LATLONG),
    'Dropoff_latitude': ('Dropoff_Lat', STRUCTTYPE_FOR_LATLONG),

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

    'surcharge': ('Surcharge', FloatType),

    'Extra': ('Extra_Amount', FloatType),  # might be same as surcharge
    'extra': ('Extra_Amount', FloatType),  # might be same as surcharge

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

# TODO: finish adding (new) strings from dictionary above
COLUMNS_TO_DROP = ['Total_Amount']


def load_config() -> Dict:
    '''
    Load relevant configuration items from various files
    :return: predictrip options
    '''
    from configparser import ConfigParser
    parser = ConfigParser()
    parser.read(os.path.join(PREDICTRIP_CODE_DIR_ROOT, 'config', 'predictrip', 'predictrip-site.ini'))
    config = {}

    # TODO: check for AWS credential sources in order checked by aws jars and boto
    #  (see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#configuring-credentials),
    #  then add whatever options to sparkconf-generation needed to distribute the first one present to workers. pass
    #  credentials as params to boto's client method
    # TODO: otherwise, get either AWS key from ~/.aws/credentials (also using configparser) if not specified in
    #  predictrip-site.ini
    # TODO: address possibility of sections not existing
    # TODO: store defaults in a predictrip-defaults.ini file rather than here
    config['aws_access_key'] = parser['AWS'].get('AccessKeyId')
    config['aws_secret_access_key'] = parser['AWS'].get('SecretAccessKey')

    config['s3_bucket_name'] = parser['S3'].get('BucketName', 'nyc-tlc')
    config['s3_trips_prefix'] = parser['S3'].get('TripFilePrefix', 'trip data')
    config['csv_stub_bytes'] = parser['S3'].get('CsvHeaderStubSize')

    config['geomesa_spark_jar'] = parser['Spark'].get('GeoMesaJar')

    return config


def build_spark_config(predictrip_config: dict) -> SparkConf:
    """
    Create an object representing the Spark configuration we want
    :type predictrip_config: dictionary in which we internally store configuration data loaded from preference files
    :return: pyspark.SparkConf instance
    """
    sc = SparkConf()
    sc = sc.setAppName('PredicTrip ' + os.path.basename(__file__))
    sc = sc.setMaster('spark://' + SPARK_MASTER + ':' + str(SPARK_MASTER_PORT))
    # load GeoMesa stuff
    sc = sc.set('spark.jars', predictrip_config['geomesa_spark_jar'])
    # add support for s3 URIs
    # TODO: try with amazon's aws library alone (no hadoop-aws)
    # TODO: try aws sdk ver 2, which is supposed to have better performance
    # TODO: try with hadoop's aws alone
    sc = sc.set('spark.jars.packages', 'com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7')
    return sc


def parse_key(key: str) -> List[Union[str, int]]:
    """
    Parse S3 key into the components we need
    :param key: key of S3 object (string)
    :return: list of key, filename, type, year, and month, with the latter two cast to integers
    """
    fn = os.path.basename(key)
    # specifying maxsplit in these spares it having to read tail ends of strings
    [kind, junk, rest] = fn.split('_', 2)
    # rest here takes form YYYY-MM.csv
    # TODO: consider stripping .csv from tail and using date string parsing on remainder to accommodate future changes
    #  in formatting of date portion
    [year, rest] = rest.split('-', 1)
    # could just grab first two chars, but this way we don't assume leading zero on month will continue
    [month, junk] = rest.split('.', 1)
    return [key, fn, kind, int(year), int(month)]


# TODO: this function will need to be modified to work for rows of a Spark DataFrame/RDD rather than a pandas DataFrame
def build_structtype_for_file(file: pandas.Series, s3_client: BaseClient, bucket_name: str) -> StructType:
    """
    Given a row representing a file, return a StructType object representing its schema.

    Determine this by downloading what can reasonably be expected to contain at least the whole first row of the CSV,
    and looking up the data types and any desired column renamings in a hard-coded dictionary.

    Making all metadata fields about file available to this function allows the same column name to potentially be given
    different types depending on other fields, if necessary (e.g. if data type for a given column name varies by year).

    :param file: pandas.Series instance containing the other metadata of the file (Key, Filename, Type, Year, Month)
    :param s3: boto3.resources.base.ServiceResource subclass instance representing service client for S3 resource
    :return: pyspark.sql.types.StructType (list-like collection of pyspark.sql.types.StructField objects) describing the
    schema of the file represented by row
    """

    # Note of possible relevance in case of major change in implementation:
    # Name and DataType are required params of StructField constructor. If necessary, we could use instances of
    # pyspark.sql.types.NullType as dummy values, as that's used when types can't be inferred, which is only relevant
    # when inferring. (i.e. that would never be the "right" data type for us.)

    # TODO: consider whether get_csv_reader_for_stub should be a top-level function instead. would it affect parallelizability?

    def get_csv_reader_for_stub():
        # TODO: I suspect importing and referencing private module _csv for a type hint is bad form. but what should it
        #  be instead? something generic like Iterable?
        # download first N bytes. byte range is inclusive and numbered from 0.
        # (see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        # and https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35)
        resp = s3_client.get_object(Bucket=bucket_name, Key=file['Key'],
                                    Range='bytes={}-{}'.format(0, config['csv_stub_bytes'] - 1))
        # HTTP status code 206 represents successful request for partial content
        if resp['ResponseMetadata']['HTTPStatusCode'] != 206:
            raise Exception('Unexpected HTTP status code received when requesting file stub: {}. '. \
                            format(resp['ResponseMetadata']['HTTPStatusCode']) + '(Expected 206.) ' \
                            + 'S3 object key: ' + file['Key'])
        # TODO: confirm that resp['ContentType'] == 'text/csv' ?
        csv_stub_bytes = resp['Body'].read()
        # by using a csv parser we stay flexible re csv dialect, theoretical possibility of a newline inside a quoted
        # string, etc.
        # TODO: take a standard encoding string in configuration and pass it to decode().
        #  see https://docs.python.org/3.6/library/codecs.html#standard-encodings
        # TODO: provide for auto-detection of EOL char(s)
        csv_rows = csv.reader(csv_stub_bytes.decode().split("\r\n"))
        return csv_rows

    csv_rows = get_csv_reader_for_stub()
    num_rows: int = 0
    # csv_rows is an iterator, so we can't check its length without iterating through it.
    # notice that this for loop will stop after at most 2 iterations
    for row in csv_rows:
        num_rows += 1
        if num_rows == 1:
            # first row (header row) is list of column names. turn each into a StructField
            schema = [build_structfield_for_column(col_name) for col_name in row]
        elif num_rows > 1:
            # we don't need to actually read later lines. we just want to know that at least one of them was detected.
            break
    if num_rows == 0:
        raise Exception('CSV file appears to be empty: ' + file['Filename'])
    elif num_rows == 1:
        # if subsequent rows weren't found, we can't know that we got the whole of the first one.
        # future improvement: make it go back for additional chunks (without overlap) until it does get an EOL
        raise Exception('No EOL encountered in attempt to downloaded first line of file '
                        + file['Filename'] + '. HEADER_STUB_SIZE should be increased')
    return StructType(schema)


def build_structfield_for_column(col: str) -> StructField:
    """
    # Build a pyspark.sql.types.StructField instance describing the schema of a tripdata CSV column having given name
    :param col: name of column as it appears in trip data CSV
    :return: pyspark.sql.types.StructField
    """
    # if need to re-add file param, its hint: file: Series
    # and docstring line for it: :param file: the pandas.Series describing that trip data CSV

    # switchyard or dictionary(s)?
    # dictionary(s) might look stupid if the flexibility they'd provide didn't end up being needed
    # switchyard would be reliant on optimization to not be highly inefficient

    try:
        # TODO: assuming no needs for differing treatment of col names differing only in case arise, add .lower() and
        #  remove upper-case dictionary entries accordingly
        attribs = ATTRIBUTES_FOR_COL[col.strip()]
    except KeyError:
        raise
    # note: the () after attribs(2) is needed to actually instantiate the class that is attribs(2)
    sf = StructField(attribs[0], attribs[1]())

    return sf


def load_tripfile_csv(row):
    # function to be applied to each row of trip file metadata "table" RDD using flatMap function, returning an object
    # Iterable over: tuples containing the next row of the file, and a metadata sidecar containing enough metadata for
    # handle_trip_record to deal with that row appropriately (e.g. the row's schema).


    pass

def handle_trip_record(row: Tuple) -> List:
    # function to be applied to each row of metadata-sidecar-tagged trip record rows from the various source CSVs.
    # in order to drop some rows, flatMap -- not map -- this function to the rows, and have this function always return
    # a list of either 0 or 1 row (no longer tupled to a sidecar). flatMap will simply not append anything when returned
    # an empty Iterable

    # drop unneeded columns

    # drop rows missing values in essential columns

    pass

# TODO: correct the floating point representation error in input data where appropriate


if __name__ == '__main__':

    config = load_config()

    # TODO: account for info in
    #  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-multiprocessing
    boto_session = boto3.session.Session()
    # client provides low-level access needed for, e.g., downloading specific byte ranges of objects, while resource
    # provides higher-level access, e.g., sparing us having to page through numerous results
    s3_client = boto_session.client('s3')  # returns a botocore.client.S3 instance
    s3_resource = boto_session.resource('s3')  # returns a boto3.resource.S3
    # TODO: try, raise own error if unable to read bucket? (more helpful message?)
    bucket = s3_resource.Bucket(config['s3_bucket_name'])

    # TODO: make function?
    # get list of files in relevant dir of bucket
    # bucket.objects.filter() returns a boto3.resources.collection.ResourceCollection
    # by converting to list we should trigger just one call to S3 API, unlike iterating over collection
    # see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/collections.html#when-collections-make-requests
    # resulting objs is list of s3.ObjectSummary instances
    objs = list(bucket.objects.filter(Prefix=config['s3_trips_prefix']).all())

    # Reminder:
    # a pyspark.sql.types.StructField is an object representing a single column (its name, datatype, nullability, etc.)
    # a pyspark.sql.types.StructType is a list of such StructFields, representing a whole schema

    # create a pandas.DataFrame to hold metadata about each of the trip data csv files

    # collect metadata as (vertical) list of (horizontal) lists (rows), then turn into a DataFrame once. should be more
    # efficient than iteratively appending to DataFrame (see "Notes" on
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.append.html#pandas-dataframe-append)
    cols = ['Key', 'Filename', 'Type', 'Year', 'Month']
    # create corresponding StructType that'll be needed for each time_period_table later
    # TODO: create this programmatically from cols above to help ensure ongoing consistency
    trip_files_schema = StructType([StructField('Key', StringType(), nullable=False),
                                    StructField('Filename', StringType(), nullable=False),
                                    StructField('Type', StringType(), nullable=False),
                                    StructField('Year', ShortType(), nullable=False),
                                    StructField('Month', ByteType(), nullable=False)])
    # parse filename into year, month, and trip type
    # would be nice to declare datatype of each column, but constructor only accepts a singleton dtype argument
    trip_files = [parse_key(obj.key) for obj in objs]
    trip_files = pandas.DataFrame(trip_files, columns=cols)

    # exclude undesired tripfiles now, using labeled columns, rather than filenames, which are less convenient
    # first pass: limit to those with usable lat and long columns: green and yellow, through the first half of 2016
    undesired_indices = trip_files[~trip_files['Type'].isin(['green', 'yellow']) |
                                   (trip_files['Year'] > 2016) |
                                   ((trip_files['Year'] == 2016) & (trip_files['Month'] > 6))].index
    trip_files.drop(undesired_indices, axis=0, inplace=True)

    # TODO: is pandas optimized enough under the hood that sorting first would speed the lookups involved in the drops?
    #  assuming not, more efficient to reduce dataset size before sorting
    trip_files.sort_values(['Year', 'Month'], axis=0, ascending=True, inplace=True)

    sparkconf = build_spark_config(config)
    # separate, explicit creation of SparkContext necessary because too late for options like spark.jars and spark.jars.packages when SparkSession's getOrCreate() would be used ?
    sparkcont = SparkContext.getOrCreate(sparkconf)
    spark = SparkSession(sparkcont)

    # plan:
    # create a separate Spark DataFrame from the rows of pandas DataFrame corresponding to each chronological chunk,
    # then, for each such time period's DataFrame, apply load_tripfile_csv, handle_trip_record, cast to
    # GeoMesa datatypes, send to DB, etc.

    # If incorporating a periodic compaction cycle, it would actually make sense to create the Spark DataFrames
    # containing the metadata of the files in each chronological chunk in a python for loop, because that's what would
    # limit the workers to processing the trip records from just one time period, then pause so a compaction cycle could
    # be run, and repeat. That is, unless Spark could be made to distribute rows according to some priority and pause
    # work at the right points or something.

    # doing simplest implementation for now: one chunk (i.e. no periodic compaction cycle)
    # otherwise, at this point we would separate trip_files entries into separate pandas DataFrames for each time period
    # and store them in a chronological list (time_period_tables)
    time_period_tables = [trip_files]

    for time_period_table in time_period_tables:
        # TODO: any customization of partitioning

        # TODO: come back later to question of whether we should be using the Values versions of map/flatMap functions

        # create spark dataframe from time_period_table
        # if it'd infer schema from pandas DataFrame, it's not well documented
        tpdf = spark.createDataFrame(time_period_table, schema=trip_files_schema, verifySchema=False)
        # TODO: find out whether there's any significant wasted work in constructing DF only to use just the RDD within
        break
        # apply load_tripfile_csv to each element of tpdf's RDD with flatMap, which will return a new RDD containing
        # all the rows of those files, vertically concatenated (logically).
        # TODO: figure out whether better to use flatMapValue instead of flatMap or set preservesPartitioning=False
        all_records_in_period = tpdf.rdd.flatMap(load_tripfile_csv)

    # spark.stop()
