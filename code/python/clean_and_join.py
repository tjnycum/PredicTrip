#!/usr/bin/env python3
# coding=utf-8
#  Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

# std lib
from typing import Tuple, List, Dict, Any, Iterable, IO, Mapping
from os import path, environ
from io import BytesIO, StringIO
from argparse import ArgumentParser, Namespace, ArgumentDefaultsHelpFormatter, ArgumentTypeError
from functools import reduce
from contextlib import redirect_stdout
from logging import info, debug

# pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame, Row, Column
from pyspark.sql.types import StructType, StructField

# geomesa_pyspark
import geomesa_pyspark

# predictrip
from common import load_config, get_boto_session, get_s3_client, build_structtype_for_file, DESIRED_COLUMNS, \
    USE_INTERMEDIATE_FILE, INTERMEDIATE_FORMAT, INTERMEDIATE_DIRS, INTERMEDIATE_USE_S3, INTERMEDIATE_COMPRESSION
from util import decompress_string

TESTING = True
TESTING_RECORD_LIMIT_PER_CSV = None
# NOTE: any such file already existing will have its contents replaced
TESTING_EXPLAIN_FILE = path.join(environ['HOME'], 'spark_explanation_most_recent.txt')


def get_spark_config(predictrip_config: Mapping[str, Any]) -> SparkConf:
    """
    Create an object representing the Spark configuration we want

    :type predictrip_config: mapping in which we internally store configuration data loaded from preference files
    :return: pyspark.SparkConf instance
    """
    # NOTE: contrary to https://www.geomesa.org/documentation/user/spark/pyspark.html#using-geomesa-pyspark, use of
    # geomesa_pyspark.configure() no longer necessary since Spark 2.1, as long as you tell spark to include the
    # geomesa_pyspark python module some other way (e.g. spark.files)

    sc = SparkConf()
    sc = sc.setAppName('PredicTrip ' + path.basename(__file__))
    # FIXME: the following doesn't seem to be effective
    sc = sc.setAll([('fs.s3a.awsAccessKeyId', predictrip_config['aws_access_key_id']),
                    ('fs.s3a.awsSecretAccessKey', predictrip_config['aws_secret_access_key'])])
    # TODO: get any other spark options from predictrip_config and add them to sc here — needed if we were to stop
    #  specifying them in spark-defaults.conf (or having them preinstalled on workers with pip)
    # such options would include: spark.jars, spark.jars.packages, spark.files
    if 'spark.executor.cores' in predictrip_config:
        sc = sc.set('spark.executor.cores', predictrip_config['spark.executor.cores'])
    return sc


def get_geomesa_datastore_options(predictrip_config: Mapping[str, Any]) -> Dict[str, str]:
    # FIXME: docstring
    return {'hbase.instance.id': predictrip_config['hbase_instance_id'],
            'hbase.catalog': predictrip_config['geomesa_catalog']}


def unpack_table(string: str) -> List[str]:
    """
    Unpack string received for metadata_table parameter into list of the JSON strings that represent individual records

    :type string: putative "linear JSON" encoding of metadata table, compressed by common.compress_string
    :return: list of individually JSON-encoded record strings
    """
    if type(string) is not str:
        raise ArgumentTypeError('not a string')
    try:
        string = decompress_string(string)
    except Exception:
        raise Exception('error decoding compressed string')
    debug('Before splitlines: ' + string)
    # if string contains just a single line, splitlines will return a list containing one string, which is exactly what
    # we need to pass pyspark in that case
    string_list = string.splitlines()
    debug('After splitlines: ' + string)
    # at this point we could try to validate each line as being valid JSON by attempting to read it using the json
    # package, but pyspark should report any problems with the JSON it receives anyway, and we wouldn't want to reject
    # any JSON the json package didn't like if psypark might have wider compatibility
    return string_list


def get_csv_stub(key: str, s3_client, config: Mapping[str, Any]) -> IO:
    """
    Get a file-like object containing the first config['csv_stub_bytes'] bytes of an S3 object.

    :param key: S3 key of the file object
    :param s3_client: Boto3 S3 Client instance with which to download stub
    :param config: mapping of predictrip configuration items
    :return: File-like object, open and ready to read
    """

    file = BytesIO()

    # byte range is inclusive and numbered from 0.
    # (see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
    # and https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35)
    resp = s3_client.get_object(Bucket=config['s3_bucket_name'], Key=key,
                                Range='bytes={}-{}'.format(0, config['csv_stub_bytes'] - 1))

    # HTTP status code 206 represents successful request for partial content
    if resp['ResponseMetadata']['HTTPStatusCode'] != 206:
        raise Exception('Unexpected HTTP status code received when requesting file stub: {}. '
                        .format(resp['ResponseMetadata']['HTTPStatusCode'])
                        + '(Expected 206.) S3 object key: ' + key)
    # TODO (future): verify that resp['ContentType'] == 'text/csv', as canary of potential reporting changes

    file.writelines(resp['Body'])
    file.seek(0)
    return file


def get_struct_type_for_s3_key(key: str, s3_client, config: Mapping[str, Any]) -> StructType:
    """
    Build a PySpark StructType describing the file having a given S3 object key

    :param key: key of the S3 object that is the file
    :param s3_client: Boto3 S3 Client instance with which to access file
    :param config: mapping of predictrip configuration items
    :return: StructType describing the schema of the file having the given key
    """
    # derive schema from column names in CSV file stub, checking for at least one EOL to confirm we downloaded a large-
    # enough stub
    # TODO: if this ends up being needed to be called multiple times per file, memoize it. (create class holding a
    #  dict(key: struct_type) and call a get method of it that only calls this func if output of previous call for key
    #  isn't already in dict.)
    stub = get_csv_stub(key, s3_client, config)
    return build_structtype_for_file(stub, verify_eol=True)


def get_metadata_data_frame(metadata_table: str, spark: SparkSession, spark_cont: SparkContext) -> DataFrame:
    # FIXME: delete if never ends up being used
    # FIXME: docstring
    # create spark data frame from metadata_table, inferring schema from JSON
    # NOTE: spark.read.json expects a list of single-record JSON strings (so-called linear JSON) (see
    # https://stackoverflow.com/a/49676143 and
    # https://spark.apache.org/docs/latest/sql-data-sources-json.html#tab_python_0)

    # alternative creating from pandas dataframe (might be useful again if this script is made to once again be called
    # by ingest.py directly):
    #     schema = StructType([StructField(col[0], col[1](), nullable=False) for col in TRIPFILE_METADATA_COLS])
    #     df = spark.createDataFrame(metadata_table, schema=schema, verifySchema=False)

    # TODO: if we're about to immediately use rdd.flatMap, then ideally we wouldn't bother creating a DF here. if we
    #  stopped at parallelizing the JSON list here, the workers in a mapped function would need to parse those JSON
    #  string RDD elements enough to get the key and any other metadata they needed about the files. Then again, perhaps
    #  it would be more efficient to parse all the JSON strings in one place on the master than to have each worker use
    #  the json library to parse each individually?

    # if what's passed to json here is not an RDD, it'll be assumed to be paths of files, rather than the JSON itself
    return spark.read.json(spark_cont.parallelize(metadata_table))


def get_data_frame_for_csv(key: str, s3_client, spark: SparkSession, config: Mapping[str, Any]) -> DataFrame:
    # FIXME: docstring

    # create DataFrame, specifying schema instead of inferring, for performance reasons
    source_filename = 's3a://' + config['s3_bucket_name'] + '/' + key
    schema = get_struct_type_for_s3_key(key, s3_client, config)
    # TODO: use a parser to determine timestampFormat from appropriate column of downloaded stub, to
    #  accommodate future formatting changes
    df = spark.read.csv(source_filename, multiLine=True, header=True, nullValue='',
                        timestampFormat='yyyy-MM-dd HH:mm:ss',
                        schema=schema, inferSchema=False, enforceSchema=True)
    if TESTING and TESTING_RECORD_LIMIT_PER_CSV is not None:
        df = df.limit(TESTING_RECORD_LIMIT_PER_CSV)
    return df


def select_new_data_frame(df: DataFrame, cols: List[str]) -> DataFrame:
    # FIXME: docstring

    # TODO (later): implement broadcast join with taxi zone DataFrame containing pre-calculated centroids
    # for each of Pickup and Dropoff:
    #     if [ df has a TZ col instead of a Lat-Long pair]:
    #          do join with broadcast TZ DF to get Lat and Long cols
    #     else:
    #          use withColumn to add TZ col of NULL literals

    # retain only the columns we're interested in
    return df.selectExpr(cols)


def uniformize_data_frames(data_frames: List[DataFrame]) -> List[DataFrame]:
    # TODO: docstring
    # Use SQL statements to extract a uniform set of columns from each of the input data frames, which can and do vary
    # in structure.
    # By making this function take a list of DataFrames (rather than an individual DataFrame and calling it in a list
    # generation), this function could potentially examine them all to dynamically determine what the uniform
    # structure should be. We don't need this ability right now, though. Instead we can simply derive the uniform
    # structure from DESIRED_COLUMNS.

    # extract list of new_name strings from DESIRED_COLUMNS
    cols_to_select = [new_name for ((new_name, data_type), old_names) in DESIRED_COLUMNS]

    data_frames = [select_new_data_frame(df, cols_to_select) for df in data_frames]
    return data_frames


def process_files_concatenated(metadata: str, spark: SparkSession, config: Mapping[str, Any]) -> None:
    # using SQL and DataFrames, trusting Catalyst's optimizations to not get bogged down by number of input DataFrames
    # FIXME: docstring

    # import from pandas locally to avoid accidental references to it from any code run on workers
    from pandas import read_json

    # read metadata into pandas data frame via in-memory file-like object
    metadata = read_json(StringIO('\n'.join(metadata)), orient='records', lines=True)

    boto_session = get_boto_session(config)
    s3_client = get_s3_client(boto_session)

    data_frames = [get_data_frame_for_csv(key, s3_client, spark, config)
                   for key, filename in zip(metadata['Key'], metadata['Filename'])]

    # TODO: consider filtering each member of data_frames individually here, so that the columns to be filtered can be
    #  declared not nullable in the schema, which could potentially allow the future join within uniformize_data_frames
    #  below to work more more efficiently. downside is that the spark plan output might be made even more unreadable.

    # standardize the structures of the data frames, then get their union
    data_frames = uniformize_data_frames(data_frames)
    df = reduce(DataFrame.union, data_frames)

    # drop rows with missing or invalid values in critical columns
    # TODO: determine whether clever use of options available during data frame creation from input CSVs could be used
    #  to achieve some of this filtering.
    #  see https://spark.apache.org/docs/2.4.4/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv
    # filter expressions can be either python expressions on column objects or SQL expressions. using SQL expressions
    # for greater code portability, and in case Catalyst wouldn't otherwise figure out that all the rows can be
    # processed completely independently
    # ref https://spark.apache.org/docs/2.4.4/api/sql/index.html
    # I tried dropna() instead of filter() to see if Spark would correctly infer that resulting columns were no longer
    # nullable, but it didn't
    df = df.filter('! ('
                   'isnull(Pickup_DateTime) or year(Pickup_DateTime) < 2009 or Pickup_DateTime > now() '
                   'or isnull(Pickup_Latitude) or Pickup_Latitude == 0 or abs(Pickup_Latitude) > 90 '
                   'or isnull(Dropoff_Latitude) or Dropoff_Latitude == 0 or abs(Dropoff_Latitude) > 90 '
                   'or isnull(Pickup_Longitude) or Pickup_Longitude == 0 or abs(Pickup_Longitude) > 180 '
                   'or isnull(Dropoff_Longitude) or Dropoff_Longitude == 0 or abs(Dropoff_Longitude) > 180 '
                   ')')

    if not USE_INTERMEDIATE_FILE:
        # register GeoMesa extensions (e.g. the SQL functions we'll need) in SparkSession
        geomesa_pyspark.init_sql(spark)

    # When saving via geomesa_pyspark, the default feature ID construction can be overridden by including a column named
    # "__fid__" that contains the value to be used for each feature

    # do all the calculations and schema changes desired in a single SQL SELECT statement
    # Spark builtins:
    #     ifnull(a, b)                  https://spark.apache.org/docs/2.4.4/api/sql/index.html#ifnull
    #     date_trunc(fmt, timestamp)    https://spark.apache.org/docs/2.4.4/api/sql/index.html#date_trunc
    #         note: date_trunc treats Monday as the first day of the week, following ISO 8601
    #     unix_timestamp(timestamp)     https://spark.apache.org/docs/2.4.4/api/sql/index.html#unix_timestamp
    # GeoMesa-JTS:
    #     st_point(x, y)        https://www.geomesa.org/documentation/user/spark/sparksql_functions.html#st-point

    if USE_INTERMEDIATE_FILE:
        # write to file for DB to ingest
        new_column_exprs = [
            'Dropoff_Longitude',
            'Dropoff_Latitude',
            'ifnull(Passenger_Count, 1) AS Passenger_Count',
            'Pickup_Longitude',
            'Pickup_Latitude',
            'unix_timestamp(Pickup_DateTime) AS Pickup_SecondOfUnixEpoch',
            "int(unix_timestamp(Pickup_DateTime) - unix_timestamp(date_trunc('week', Pickup_DateTime)))"
            ' AS Pickup_SecondOfWeek'
        ]
    else:
        # accommodate the unintentional requirement of geomesa_pyspark's/Spark's DF.save() that the columns be in
        # lexicographical order
        new_column_exprs = [
            'st_point(Dropoff_Longitude, Dropoff_Latitude) AS Dropoff_Location',
            'ifnull(Passenger_Count, 1) AS Passenger_Count',
            'Pickup_DateTime AS Pickup_DateTime',
            'st_point(Pickup_Longitude, Pickup_Latitude) AS Pickup_Location',
            "int(unix_timestamp(Pickup_DateTime) - unix_timestamp(date_trunc('week', Pickup_DateTime)))"
            ' AS Pickup_SecondOfWeek'
        ]
        # NOTE: to provide for summarization in future, add a Trip_Count Integer attribute and cast Passenger_Count to
        # Float (to hold the average passenger count for the summarized trips)
        #    '1 AS Trip_Count'
        #    'float(ifnull(Passenger_Count, 1)) AS Passenger_Count'

    df = df.selectExpr(new_column_exprs)

    debug('Schema of data frame as output to/for database: ' + str(df.schema))

    if TESTING and TESTING_EXPLAIN_FILE is not None:
        # Save query plan on driver for analysis
        with open(TESTING_EXPLAIN_FILE, mode='wt') as file:
            # have to redirect stdout, as df.explain does not return the string. it prints it to stdout and returns None
            with redirect_stdout(file):
                df.explain(extended=True)
            info('Spark extended query explanation saved to ' + TESTING_EXPLAIN_FILE)

    if USE_INTERMEDIATE_FILE:
        # save to an intermediate file set to be ingested by geomesa_hbase, bypassing geomesa_pyspark issues
        df_writer = df.write.mode('overwrite').option('compression', INTERMEDIATE_COMPRESSION)
        if INTERMEDIATE_FORMAT not in ['avro', 'parquet', 'csv']:
            raise NotImplementedError
        df_writer.format(INTERMEDIATE_FORMAT)
        if INTERMEDIATE_FORMAT == 'csv':
            df_writer.options({'header': True, 'nullValue': 'null',
                               'timestampFormat': 'yyyy-MM-dd HH:mm:ss'})
        if INTERMEDIATE_USE_S3:
            url_start = 's3a://' + config['s3_bucket_name']
        else:
            url_start = 'hdfs://' + config['hadoop_namenode_host'] + ':' + str(config['hadoop_namenode_port'])
        df_writer.save('/'.join([url_start, *INTERMEDIATE_DIRS]))

        # the following should be unnecessary as long as code that would read the file (set) next would be derailed by
        # an exception thrown by the above (which potentially won't be the case once orchestrated by airflow)
        # if INTERMEDIATE_USE_S3:
        #     # verify success of save by checking for _SUCCESS file
        #     resp: Dict = s3_client.list_objects_v2({'Bucket': config['s3_bucket_name'], 'MaxKeys': 1,
        #                                             'Prefix': '/'.join([*INTERMEDIATE_DIRS, '_SUCCESS'])})
        #     if resp['Key_Count'] < 1:
        #         raise Exception('_SUCCESS file (signalling completion of save by Spark) not found')

    else:
        # insert directly into DB using GeoMesa's API
        df_writer = df.write.format('geomesa').options(**get_geomesa_datastore_options(config)) \
            .option('geomesa.feature', config['geomesa_feature'])
        df_writer.save()


def parse_args() -> Namespace:
    # FIXME: docstring
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('metadata', type=unpack_table, help='table describing the CSVs to be processed, as "linear"'
                                                            ' JSON, zlib-compressed and base64-encoded')
    return parser.parse_args()


def main():
    args = parse_args()
    # if args were to be consolidated with anything else (e.g. env vars, config files, etc), this would be the time
    metadata = args.metadata

    config = load_config()

    with SparkContext.getOrCreate(get_spark_config(config)) as spark_cont:
        spark = SparkSession(spark_cont)

        # TODO: correct the apparent floating point representation error in input data where appropriate?

        process_files_concatenated(metadata, spark, config)

        spark.stop()


if __name__ == '__main__':
    main()
