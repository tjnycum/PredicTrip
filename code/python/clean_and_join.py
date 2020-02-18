#!/usr/bin/env python3
# coding=utf-8
#  Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

# NOTE: many imports in this file in particular are done at the function level to minimize the potential for data
# unnecessarily being sent to spark workers when functions in these files are mapped to them

# TODO: use logging module! https://docs.python.org/3.6/howto/logging.html

# std lib
from typing import Tuple, List, Dict, Any, Iterable, IO
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, ArgumentTypeError

# pyspark
from pyspark import RDD, SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField

# predictrip
from common import load_config, get_s3_client, get_s3_resource, get_s3_bucket, stdout_redirected_to \
    build_structfield_for_column, build_structtype_for_file, decompress_string, \
    PREDICTRIP_REPO_ROOT, SPARK_DATATYPE_FOR_LATLONG, ATTRIBUTES_FOR_COL, UNDESIRED_COLUMNS, \
    TRIPFILE_METADATA_COLS, TRIPFILE_METADATA_KEY_COL_IDX, TRIPFILE_METADATA_FILENAME_COL_IDX

from os import path, environ


TESTING = True
TESTING_RECORD_LIMIT_PER_CSV = None
TESTING_EXPLAIN_FILE = path.join(environ['HOME'], 'spark_explanation_most_recent.txt')
# any such file already existing will be appended


def build_spark_config(predictrip_config: Dict[str, Any]) -> SparkConf:
    """
    Create an object representing the Spark configuration we want

    :type predictrip_config: dictionary in which we internally store configuration data loaded from preference files
    :return: pyspark.SparkConf instance
    """
    # TODO: decide whether to keep specifying jars and such in here or in spark-defaults.conf
    from os import path

    sc = SparkConf()
    sc = sc.setAppName('PredicTrip ' + path.basename(__file__))
    # load GeoMesa stuff — spark-defaults.sh should be taking care of this
    # sc = sc.set('spark.jars', predictrip_config['spark_geomesa_jar'])
    # add support for s3 URIs
    # TODO: try with amazon's aws library alone (no hadoop-aws)
    # TODO: try aws sdk ver 2, which is supposed to have better performance
    # TODO: try with hadoop's aws alone
    sc = sc.set('spark.jars.packages', 'com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7')
    if 'spark.executor.cores' in predictrip_config:
        sc = sc.set('spark.executor.cores', predictrip_config['spark.executor.cores'])
    return sc


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


def get_csv_stub(key: str, s3_client, config: Dict[str, Any]) -> IO:
    """
    Get a file-like object containing the first config['csv_stub_bytes'] bytes of an S3 object.

    :param key: S3 key of the file object
    :param s3_client: Boto3 S3 Client instance with which to download stub
    :param config: dictionary of predictrip configuration items
    :return: File-like object, open and ready to read
    """
    from io import BytesIO

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


def get_struct_type_for_s3_key(key: str, s3_client, config: Dict[str, Any]) -> StructType:
    """
    Build a PySpark StructType describing the file having a given S3 object key

    :param key: key of the S3 object that is the file
    :param s3_client: Boto3 S3 Client instance with which to access file
    :param config: predictrip configuration dictionary
    :return: StructType describing the schema of the file having the given key
    """
    # derive schema from column names in CSV file stub, checking for at least one EOL to confirm we downloaded a large
    # enough stub
    # TODO: if this ends up being needed to be called multiple times per file, memoize it. (create class holding a
    #  dict(key: structtype) and call a get method of it that only calls this func if output of previous call for key
    #  isn't already in dict.)
    stub = get_csv_stub(key, s3_client, config)
    return build_structtype_for_file(stub, verify_eol=True)


def get_metadata_data_frame(metadata_table: str) -> DataFrame:
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


def load_csv_contents(metadata_row: Row, config: Dict[str, Any]) -> List[Tuple[Tuple, str]]:
    # alternative to load_and_standardize_csv_contents* that returns heterogeneously structured CSV rows in tuples with
    # whatever metadata will be needed by a subsequently mapped function to then standardize them.

    pass


def load_and_standardize_csv_contents_pandas(metadata_row: Row, config: Dict[str, Any]) -> List:
    # function to be applied to each row of trip file metadata "table" RDD using flatMap function, returning an object
    # that iterates over the rows of the file standardized with the help of Pandas
    # Problem is Pandas can't read from S3 directly. So we'd have to download to a temporary file on disk using Boto3,
    # and only once that completed would processing of it begin. I believe (hope) that would not be true of Spark.

    pass


def get_data_frame_for_csv(key: str, s3_client, config: Dict[str, Any]) -> DataFrame:
    # TODO: docstring

    # create DataFrame, specifying schema instead of inferring, for performance reasons
    source_filename = 's3a://' + config['s3_bucket_name'] + '/' + key
    schema = get_struct_type_for_s3_key(key, s3_client, config)
    # TODO: use a parser to determine timestampFormat from appropriate column of downloaded stub, to
    #  accommodate future formatting changes
    # TODO: performance impact of ignore{Leading,Trailing}WhiteSpace?
    # TODO: figure out whether some needed option to read.csv not being used is what's resulting in Lat/Long fields
    #  losing precision. we'd probably be getting rid of that excessive precision anyway, but the behavior should be
    #  understood
    df = spark.read.csv(source_filename, multiLine=True, header=True, nullValue=None,
                        # ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True,
                        timestampFormat='yyyy-MM-dd HH:mm:ss',
                        schema=schema, inferSchema=False, enforceSchema=True)
    if TESTING and TESTING_RECORD_LIMIT_PER_CSV is not None:
        df = df.limit(TESTING_RECORD_LIMIT_PER_CSV)
    return df


def select_new_data_frame(df: DataFrame) -> DataFrame:
    # TODO: docstring

    # drop columns we're not currently interested in
    df = df.drop(*UNDESIRED_COLUMNS)

    # Alternative approach that would be better for maintainability (in the face of potential future changes to the
    # TLC's reporting). But it might be less convenient for us. TBD when implementing the joining with TZ data
    # DESIRED_COLUMNS = ['Pickup_DateTime', 'Dropoff_DateTime', 'Passenger_Count',
    #                    'Pickup_Longitude', 'Pickup_Latitude', 'Dropoff_Longitude', 'Dropoff_Latitude']
    # df = df.selectExpr(DESIRED_COLUMNS)

    # Hopefully Spark is "smart" enough that the choice wouldn't affect the query plan.
    # TODO: test that

    # TODO: handle joining with broadcasted taxi zone DataFrame and alternative default TZ_ID cols, as necessary
    # for each of Pickup and Dropoff:
    #     if [ df has a TZ col instead of a Lat-Long pair]:
    #          do join with broadcasted TZ DF to get Lat and Long cols
    #     else:
    #          add a dummy TZ col of Nones

    return df


def uniformize_data_frames(data_frames: List[DataFrame], config: Dict[str, Any]) -> List[DataFrame]:
    # TODO: docstring
    # Use a SQL SELECT statement (and possibly joins) to extract a uniform set of columns from each of the input data
    # frames of varying structure.
    # By making this function take a list (rather than an individual DataFrame and calling it in a list generation),
    # this function has an opportunity to potentially examine them all to dynamically determine what the uniform
    # structure should be. However I don't think we need this ability right now.

    data_frames = [select_new_data_frame(df) for df in data_frames]
    return data_frames


def process_files_concatenated(metadata: str, config: Dict[str, Any]) -> None:
    # less naive, but still using SQL and DataFrames, dependent upon good work by Spark's optimizer
    # TODO: docstring

    from pandas import read_json
    from io import StringIO
    from functools import reduce
    from copy import copy

    # read metadata into pandas data frame via in-memory file-like object
    metadata = read_json(StringIO('\n'.join(metadata)), orient='records', lines=True)

    from boto3 import session
    boto_session = session.Session()
    s3_client = get_s3_client(boto_session)

    data_frames = [get_data_frame_for_csv(key, s3_client, config)
                   for key, filename in zip(metadata['Key'], metadata['Filename'])]

    # standardize the structures of the data frames, then get their union
    # TODO: find out performance penalty of using unionByName. alternative would be to use a list of the columns that we
    #  DO want in a select statement in uniformize_data_frames to get consistent column ordering, instead of the current
    #  dropping of what we don't want (leaving resulting ordering unknown)
    data_frames = uniformize_data_frames(data_frames, config)
    df = reduce(DataFrame.unionByName, data_frames)

    # filter rows missing values in critical columns
    # TODO: determine whether clever use of options available during data frame creation from input CSVs could be used
    #  to achieve some of this filtering.
    #  see https://spark.apache.org/docs/2.4.4/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader
    # filter expressions can be either python operations on column objects or SQL expressions. using SQL expressions in
    # case it's otherwise not smart enough to figure out that all the rows can be processed completely independently,
    # and for code portability
    # ref https://spark.apache.org/docs/2.4.4/api/sql/index.html
    df = df.filter('! (isnull(Pickup_DateTime) or Pickup_Latitude == 0 or Pickup_Longitude == 0 ' +
                   'or Dropoff_Latitude == 0 or Dropoff_Longitude == 0)')

    # TODO: examine spark execution plans. I suspect it might be doing the column creation before the filtering, which
    #  means pointlessly calculating new column values for rows that will only be thrown away. also using those
    #  execution plans, ensure the order of the changes made below doesn't matter. if it does, order them accordingly

    # add new columns containing pickup day of the week and hour of the day
    # sql func: dayofweek(timestamp)    https://spark.apache.org/docs/2.4.4/api/sql/index.html#dayofweek
    # sql func: hour(datetime)          https://spark.apache.org/docs/2.4.4/api/sql/index.html#hour
    new_column_exprs = copy(df.columns)
    new_column_exprs.insert(1, 'dayofweek(Pickup_DateTime) AS Pickup_Day')
    new_column_exprs.insert(2, 'hour(Pickup_DateTime) AS Pickup_Hour')
    df = df.selectExpr(new_column_exprs)

    if TESTING:
        # Save query plan on driver for analysis
        with open(TESTING_EXPLAIN_FILE, mode='at') as file:
            # have to redirect stdout, as df.explain does not return the string. it prints it to stdout and returns None
            with stdout_redirected_to(file):
                df.explain(extended=True)
            print('Spark query explanation saved to ' + TESTING_EXPLAIN_FILE)

    # save to one big merged file set in S3 for the hand-off to Java, for the time being
    df.write.parquet(path='s3a://' + config['s3_bucket_name'] + '/intermediate/time_period',
                     mode='overwrite', compression='none')
    # df.write.csv(path='s3a://' + config['s3_bucket_name'] + '/intermediate/time_period',
    #              mode='overwrite', header=True, nullValue='null', timestampFormat='yyyy-MM-dd HH:mm:ss')
    # TODO: confirm creation of _SUCCESS file in S3. if not there, don't reattempt, though, just give error, because the
    #  whole pipeline would effectively have to be rerun from the beginning anyway. so let a human investigate and rerun
    # have to use boto? or can hadoop's built-in support be used?


if __name__ == '__main__':
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('metadata_table', type=unpack_table, help='metadata about the CSVs to be processed, as "linear"'
                                                                  ' JSON, zlib-compressed and base64-encoded')
    args = parser.parse_args()
    metadata_table = args.metadata_table

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

    # explicitly creating a SparkContext first necessary because too late for options like spark.jars and
    # spark.jars.packages when SparkSession's getOrCreate() would be used ?
    with SparkContext.getOrCreate(build_spark_config(config)) as spark_cont:
        spark = SparkSession(spark_cont)

        # TODO: correct the apparent floating point representation error in input data where appropriate?

        process_files_concatenated(metadata_table, config)

        # use pyspark DataFrame for easy loading and manipulation in terms of labeled columns
        # see https://datascience.stackexchange.com/a/13302, which includes pure RDD alternative

        spark.stop()
