#!/usr/bin/env python3
# coding=utf-8
#  Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

# NOTE: many imports in this file in particular are done at the function level to minimize the potential for data
# unnecessarily being sent to spark workers when functions in these files are mapped to them

# TODO: use logging module! https://docs.python.org/3.6/howto/logging.html

# std lib
from typing import Tuple, List, Dict, Any, Iterable, IO, Mapping
from os import path, environ
from io import BytesIO, StringIO
from argparse import ArgumentParser, Namespace, ArgumentDefaultsHelpFormatter, ArgumentTypeError

# pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame, Row, Column
from pyspark.sql.types import StructType, StructField

# geomesa_pyspark
import geomesa_pyspark

# predictrip
from common import load_config, get_boto_session, get_s3_client, stdout_redirected_to, \
    build_structfield_for_column, build_structtype_for_file, decompress_string, \
    ATTRIBUTES_FOR_COL, UNDESIRED_COLUMNS, \
    USE_INTERMEDIATE_FILE, INTERMEDIATE_FORMAT, INTERMEDIATE_DIRS, INTERMEDIATE_USE_S3, INTERMEDIATE_COMPRESSION

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


def get_geomesa_datastore_connection_config(predictrip_config: Mapping[str, Any]) -> Dict[str, str]:
    # FIXME: docstring
    return {'hbase.instance.id': predictrip_config['hbase_instance_id'],
            'hbase.catalog': predictrip_config['geomesa_catalog']}


def unpack_table(string: str, debugging=False) -> List[str]:
    """
    Unpack string received for metadata_table parameter into list of the JSON strings that represent individual records

    :type string: putative "linear JSON" encoding of metadata table, compressed by common.compress_string
    :param debugging: whether to include some output potentially helpful in debugging
    :return: list of individually JSON-encoded record strings
    """
    if type(string) is not str:
        raise ArgumentTypeError('not a string')
    try:
        string = decompress_string(string)
    except Exception:
        raise Exception('error decoding compressed string')
    if debugging:
        print('before splitlines:')
        print(string)
    # if string contains just a single line, splitlines will return a list containing one string, which is exactly what
    # we need to pass pyspark in that case
    string_list = string.splitlines()
    if debugging:
        print('after splitlines:')
        print(string_list)
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
    # TODO: confirm that resp['ContentType'] == 'text/csv' ?

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


def select_new_data_frame(df: DataFrame) -> DataFrame:
    # FIXME: docstring

    # drop columns we're not currently interested in
    df = df.drop(*UNDESIRED_COLUMNS)

    # Alternative approach that would be better for maintainability (in the face of potential future changes to the
    # TLC's reporting), but might be less convenient for us. TBD when implementing the joining with TZ data
    # DESIRED_COLUMNS = ['Pickup_DateTime', 'Pickup_Day', 'Pickup_Hour', 'Dropoff_DateTime', 'Passenger_Count',
    #                    'Pickup_Longitude', 'Pickup_Latitude', 'Dropoff_Longitude', 'Dropoff_Latitude']
    # df = df.selectExpr(DESIRED_COLUMNS)

    # TODO: handle joining with broadcasted taxi zone DataFrame and alternative default TZ_ID cols, as necessary
    # for each of Pickup and Dropoff:
    #     if [ df has a TZ col instead of a Lat-Long pair]:
    #          do join with broadcasted TZ DF to get Lat and Long cols
    #     else:
    #          add a dummy TZ col of Nones

    return df


def uniformize_data_frames(data_frames: List[DataFrame]) -> List[DataFrame]:
    # TODO: docstring
    # Use SQL statements to extract a uniform set of columns from each of the input data frames, which can and do vary
    # in structure.
    # By making this function take a list (rather than an individual DataFrame and calling it in a list generation),
    # this function has an opportunity to potentially examine them all to dynamically determine what the uniform
    # structure should be. We don't need this ability right now, though.

    data_frames = [select_new_data_frame(df) for df in data_frames]
    return data_frames


def process_files_concatenated(metadata: str, spark: SparkSession, config: Mapping[str, Any]) -> None:
    # using SQL and DataFrames, trusting Catalyst's optimizations to not get bogged down by number of input DataFrames
    # FIXME: docstring

    from pandas import read_json
    from functools import reduce

    # read metadata into pandas data frame via in-memory file-like object
    metadata = read_json(StringIO('\n'.join(metadata)), orient='records', lines=True)

    boto_session = get_boto_session(config)
    s3_client = get_s3_client(boto_session)

    data_frames = [get_data_frame_for_csv(key, s3_client, spark, config)
                   for key, filename in zip(metadata['Key'], metadata['Filename'])]

    # standardize the structures of the data frames, then get their union
    # TODO: find out performance penalty of using unionByName. alternative would be to use a list of the columns that we
    #  DO want in a select statement in uniformize_data_frames to get consistent column ordering, instead of the current
    #  dropping of what we don't want (leaving resulting ordering unknown)
    data_frames = uniformize_data_frames(data_frames)
    df = reduce(DataFrame.unionByName, data_frames)

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

    # GeoMesa would use the values of a column named __fid__ as the "feature ID" (i.e. primary key of the records).
    # GeoMesa only supports vertical partitioning ("splitting" as they call it) of tables on the basis of feature ID.
    # When feature ID is unspecified, GM defaults to using a generator that incorporates the z3 index (in a way that
    # preserves proximity relationships). See
    # https://www.geomesa.org/documentation/user/datastores/runtime_config.html#geomesa-feature-id-generator
    # An alternative would be to follow GM's general advice of using an md5 hash of the "whole record", but as GM
    # apparently doesn't actually enforce uniqueness of the feature ID, doing so wouldn't actually help us prevent
    # duplication anyway.

    # TODO: once code settled, update comment below to be consistent with it
    # add new columns containing pickup day of the week and hour and second of the day, and replace the latitude and
    # longitude float pairs with GeoMesa points
    # Spark builtins:
    #     ifnull(a, b)                  https://spark.apache.org/docs/2.4.4/api/sql/index.html#ifnull
    #     hour(timestamp)               https://spark.apache.org/docs/2.4.4/api/sql/index.html#hour
    #     weekday(timestamp)            https://spark.apache.org/docs/2.4.4/api/sql/index.html#weekday
    #       note: weekday()   maps {Mon–Sun} -> {0–6}
    #             dayofweek() maps {Sun–Sat} -> {1–7}
    #         I use weekday here b/c it follows:
    #             1. international standard (ISO 8601) re semantics of first day of week, and
    #             2. 0-based indexing — no code currently relies on this, but as it's more common among
    #                                   programming languages, including this one, it seems the safer choice
    #     date_trunc(fmt, timestamp)    https://spark.apache.org/docs/2.4.4/api/sql/index.html#date_trunc
    #     unix_timestamp(timestamp)     https://spark.apache.org/docs/2.4.4/api/sql/index.html#unix_timestamp
    # GeoMesa-JTS:
    #     st_point(x, y)        https://www.geomesa.org/documentation/user/spark/sparksql_functions.html#st-point

    # accommodate the unintentional requirement of geomesa_pyspark's/Spark's DF.save() that the columns be in
    # lexicographical order

    # NOTE: The timezone of the timestamps in the input CSVs isn't specified, but is presumably NYC local
    # time. We could store them as UTC as one might in a system serving the whole globe, but I'm choosing not to for
    # a couple reasons: 1) it would complicate (or risk confusing) the querying, and 2) it would de-adjust for DST,
    # which we probably don't want to do because DST-adjusted times are more relevant to people's travel patterns. In
    # fact, you might store in local time even in a system serving the whole globe for that reason, at least if it
    # weren't for trips spanning time zone and/or DST boundaries (spatial or temporal).
    if USE_INTERMEDIATE_FILE:
        # work around geomesa_pyspark shortcomings by writing to file for DB to ingest
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
        new_column_exprs = [
            "int(unix_timestamp(Pickup_DateTime) - unix_timestamp(date_trunc('week', Pickup_DateTime)))"
            ' AS Pickup_SecondOfWeek',
            'st_point(Dropoff_Longitude, Dropoff_Latitude) AS Dropoff_Location',
            'ifnull(Passenger_Count, 1) AS Passenger_Count',
            'Pickup_DateTime AS Pickup_DateTime',
            'st_point(Pickup_Longitude, Pickup_Latitude) AS Pickup_Location'
        ]
        # NOTE: to provide for summarization, add a Trip_Count Integer attribute and cast Passenger_Count to Float (to
        # hold the average passenger count for the summarized trips)
        #    '1 AS Trip_Count'
        #    'ifnull(Passenger_Count, 1) AS Passenger_Count'

    df = df.selectExpr(new_column_exprs)

    # TODO: log (DEBUG) this instead
    print(df.schema)

    if TESTING and TESTING_EXPLAIN_FILE is not None:
        # Save query plan on driver for analysis
        with open(TESTING_EXPLAIN_FILE, mode='wt') as file:
            # have to redirect stdout, as df.explain does not return the string. it prints it to stdout and returns None
            with stdout_redirected_to(file):
                # FIXME: this isn't preventing output to console. instead sending to both
                df.explain(extended=True)
            print('Spark query explanation saved to ' + TESTING_EXPLAIN_FILE)

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
        # an exception thrown by the above (might not be true once orchestrated by airflow?)
        # if INTERMEDIATE_USE_S3:
        #     # verify success of save by checking for _SUCCESS file
        #     resp: Dict = s3_client.list_objects_v2({'Bucket': config['s3_bucket_name'], 'MaxKeys': 1,
        #                                             'Prefix': '/'.join([*INTERMEDIATE_DIRS, '_SUCCESS'])})
        #     if resp['Key_Count'] < 1:
        #         raise Exception('_SUCCESS file (signalling completion of save by Spark) not found')

    else:
        # insert directly into DB using GeoMesa's API
        df_writer = df.write.format('geomesa').options(**get_geomesa_datastore_connection_config()) \
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

    # Spark DataFrames cannot be created or referenced inside mapped functions. It leads to the following error:
    # "_pickle.PicklingError: Could not serialize object: Exception: It appears that you are attempting to reference
    # SparkContext from a broadcast variable, action, or transformation. SparkContext can only be used on the driver,
    # not in code that it run on workers. For more information, see SPARK-5063."

    # Given that, I can think of only a few ways to do the downloading of the heterogeneously structured CSVs inside a
    # mapped function:
    # 1. load_and_standardize_csv_contents_pandas (not yet rewritten for pandas rather than spark DFs)
    #    Use pandas to still be able to homogenize the structure relatively easily by working with the columns by name.
    #    Pandas will read from S3, but will no doubt download the whole file first. (Either that or we'd have to
    #    micromanage pandas DF creation for chunks of the file at a time, if pandas allows that.) Then we might as well
    #    do all the cleaning, structural homogenization, and re-uploading to S3 in that same mapped function while
    #    they're already pandas DFs, right?
    #
    # 2. If pyspark can still read from s3 without referencing the SparkContext (which I'm skeptical of), then the lines
    #    of the CSVs could be processed progressively as they're downloaded. If built-in Spark methods of reading from
    #    S3 could not be used, then we'd have to use Boto3. Enabling the processing to begin before the whole files were
    #    downloaded would likely require multi-threading and some objects to prevent overrunning of the downloaded
    #    portions of the files. In any case, the parsing of the CSVs would probably have to be done with the csv module.
    #    The Iterable-returning method I found before was limited to returning DictReader objects, which output tuples
    #    of column name and value for each row. I'd need to make the handling vary depending on the metadata about the
    #    CSV. And since it'd already be visiting each cell of the CSV, it also might as well do the structural
    #    homogenization in the same mapped function, right? In my tentative approach to this before, I had the mapped
    #    function dynamically choosing a sub-function to call (based on which CSV it was) that would iterate over the
    #    CSV rows doing whatever hard-coded set of manipulations was appropriate for its batch of files. (That way we'd
    #    avoid lots of conditional statements being evaluated redundantly for each row of a file, within which the
    #    evaluations would always be the same anyway.) Any future joining should be built into the same single pass over
    #    each row. The logic of that one pass seems more daunting than #3 below.
    #
    # 3. load_csv_contents, standardize_csv_contents
    #    In the first (flatMapped) function, download the CSV for the file (probably having to use Boto3) and return a
    #    list of its rows, each in a tuple with a sidecar of whatever metadata would be needed by the second (mapped)
    #    function to parse the CSV of the line and homogenize its structure, probably using hard-coded logic in one of
    #    several different sub-functions, as in #2. After those two functions have been mapped, construct a DF with a
    #    newly consistent schema and re-upload to S3. In between the mapped function calls, we would want to ensure it
    #    did not do any shuffling of the partitions. With shuffling, multiple mapped functions would mean unnecessary
    #    transfer within the cluster. Any future joining would be done row-wise within standardize_csv_contents.
    #
    #   Of the above options, #3 seems like the best option. All of these would proceed as follows:
    #       df = get_metadata_data_frame(metadata_table, parallelize=True)
    #   then proceed to flatMap/map one or more functions to it (as RDD), e.g.
    #       rdd = df.rdd.flatMap(load_and_standardize_csv_contents_pandas)
    #   (Note: S3 client objects would need to be created within any mapped function downloading from S3 with Boto —
    #   See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-multiprocessing)
    #   then construct a DF around the final RDD, utilizing the newly homogeneous schema
    #       df = rdd.toDF( [schema] )
    #   and finally save that DF to one big file per executor in S3 for the hand-off to Java.

    with SparkContext.getOrCreate(get_spark_config(config)) as spark_cont:
        spark = SparkSession(spark_cont)

        # TODO: correct the apparent floating point representation error in input data where appropriate?

        process_files_concatenated(metadata, spark, config)

        # use pyspark DataFrame for easy loading and manipulation in terms of labeled columns
        # see https://datascience.stackexchange.com/a/13302, which includes pure RDD alternative

        spark.stop()


if __name__ == '__main__':
    main()
