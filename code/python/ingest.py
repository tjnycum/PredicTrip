#!/usr/bin/env python3
# coding=utf-8
#  Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

# std lib
from os import path
from typing import List, Mapping, Union, Tuple, Any
from subprocess import run

# pandas
from pandas import DataFrame

# predictrip
from common import load_config, get_boto_session, get_s3_client, get_s3_resource, get_s3_bucket, \
    build_structfield_for_column, build_structtype_for_file, compress_string, \
    SPARK_DATATYPE_FOR_LATLONG, ATTRIBUTES_FOR_COL, UNDESIRED_COLUMNS, TRIPFILE_METADATA_COLS, \
    USE_INTERMEDIATE_FILE, INTERMEDIATE_FORMAT, INTERMEDIATE_DIRS, INTERMEDIATE_USE_S3, INTERMEDIATE_COMPRESSION


TESTING = True
TESTING_CSV_LIMIT = None

# TODO: make into Ingester class


def parse_key(key: str) -> List[Union[str, int]]:
    """
    Parse S3 key into the components we need

    :param key: key of S3 object (string)
    :return: list of key, filename, type, year, and month, with the latter two cast to integers
    """
    fn = path.basename(key)
    # specifying maxsplit in these spares it having to read tail ends of strings
    [kind, junk, rest] = fn.split('_', 2)
    # rest here takes form YYYY-MM.csv
    # TODO: consider stripping .csv from tail and using date string parsing on remainder to accommodate future changes
    #  in formatting of date portion
    [year, rest] = rest.split('-', 1)
    # could just grab first two chars, but this way we don't assume leading zero on month will continue
    [month, junk] = rest.split('.', 1)
    # TODO: programmatically ensure the ordering of the following stays consistent with TRIPFILE_METADATA_COLS
    return [key, fn, kind, int(year), int(month)]


def clean_and_standardize_time_period_data(time_period_metadata: DataFrame, config: Mapping[str, str]) -> None:
    # FIXME: add docstring

    # NOTE: for compatibility with pyspark method used on the other end, it MUST be that orient='records' and lines=True
    table_json = time_period_metadata.to_json(orient='records', lines=True)
    # to avoid python/OS errors about argument length, compress
    table_compressed = compress_string(table_json)

    # construct call to spark-submit
    spark_submit = path.join(config['spark_home'], 'bin', 'spark-submit')
    # don't need to specify to spark-submit anything set in spark-defaults.conf or SparkConf
    script = path.join(config['repo_root'], 'code', 'python', 'clean_and_join.py')
    cmd = [spark_submit, script, table_compressed]
    # Note: including config['repo_root'] part of path in script is redundant of using cwd arg, but *shrug* this way
    # a little more resilient to future code changes
    # TODO: log (DEBUG) the command about to be run
    run(cmd, check=True, cwd=config['repo_root'])


def cast_and_insert_time_period_data(config: Mapping[str, str]) -> None:
    # FIXME: add docstring
    # only needed if not using geomesa_pyspark to insert to DB directly from spark

    # construct call to geomesa-hbase to ingest features from intermediate file
    geomesa = path.join(config['geomesa_home'], 'bin', 'geomesa-hbase')
    if INTERMEDIATE_COMPRESSION != 'uncompressed':
        raise NotImplementedError
    if INTERMEDIATE_USE_S3:
        url_start = 's3a://' + config['s3_bucket_name']
    else:
        url_start = 'hdfs://' + config['hadoop_namenode_host'] + ':' + str(config['hadoop_namenode_port'])
    cmd = [geomesa, 'ingest', '-c', config['geomesa_catalog'], '-C', config['geomesa_converter'],
           '-f', config['geomesa_feature'], '--run-mode', 'distributed',
           '/'.join([url_start, *INTERMEDIATE_DIRS, '*.' + INTERMEDIATE_FORMAT])]
    # TODO: log (DEBUG) the command about to be run
    run(cmd, check=True)


def simulate_time_period(time_period_table: DataFrame, config: Mapping[str, Any]) -> None:
    # FIXME: docstring

    # assuming any filtration into time periods will have preserved their internal sort
    if len(time_period_table) > 1:
        print('Time period: {first.Month}/{first.Year} – {last.Month}/{last.Year}'
              .format(first=time_period_table.iloc[0], last=time_period_table.iloc[-1]))
    else:
        print('Time period: {only.Month}/{only.Year}'.format(only=time_period_table.iloc[0]))

    if USE_INTERMEDIATE_FILE:
        print('Cleaning and standardizing time period trips')
    else:
        print('Ingesting time period trips')
    clean_and_standardize_time_period_data(time_period_table, config)
    if USE_INTERMEDIATE_FILE:
        print('Casting and inserting time period trips')
        cast_and_insert_time_period_data(config)

    # TODO (future): trigger compaction cycle

    input('Press Enter to continue with next time period')


def main():

    config = load_config()

    # TODO: break down into more functions?

    boto_session = get_boto_session(config)
    # TODO: if s3_resources continue to be used only to get bucket, merge get_s3_resource into get_s3_bucket. but wait
    #  until we've restructured as class.
    s3_resource = get_s3_resource(boto_session)
    s3_bucket = get_s3_bucket(s3_resource, config)

    # get list of files in relevant dir of bucket
    # bucket.objects.filter() returns a boto3.resources.collection.ResourceCollection
    # by converting to list we should trigger just one call to S3 API, unlike iterating over collection
    # see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/collections.html#when-collections-make-requests
    # resulting objs is list of s3.ObjectSummary instances
    objs = list(s3_bucket.objects.filter(Prefix=config['s3_trips_prefix']).all())

    # create a pandas.DataFrame to hold metadata about each of the trip data csv files

    # collect metadata as (vertical) list of (horizontal) lists (rows), then construct DataFrame. should be more
    # efficient than iteratively appending to DataFrame (see "Notes" on
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.append.html#pandas-dataframe-append)
    # parse filename into year, month, and trip type
    # would be nice to declare data type of each column, but constructor only accepts a singleton dtype argument
    trip_files = [parse_key(obj.key) for obj in objs]
    trip_files = DataFrame(trip_files, columns=[col[0] for col in TRIPFILE_METADATA_COLS])
    # TODO: convert column data types for increased efficiency of manipulations to follow ?

    # exclude undesired tripfiles now, using labeled columns, rather than filenames, which are less convenient
    # first pass: limit to those with usable lat and long columns: green and yellow, through the first half of 2016
    undesired_indices = trip_files[~trip_files['Type'].isin(['green', 'yellow']) |
                                   (trip_files['Year'] > 2016) |
                                   ((trip_files['Year'] == 2016) & (trip_files['Month'] > 6))].index
    trip_files.drop(undesired_indices, axis=0, inplace=True)

    # TODO: is pandas optimized enough under the hood that sorting first would speed the lookups involved in the drops?
    #  assuming not, more efficient to reduce dataset size before sorting
    trip_files.sort_values(['Year', 'Month'], axis=0, ascending=True, inplace=True)

    # If incorporating a periodic compaction cycle, we'd create a metadata table for the files to be ingested in each
    # cycle. The ETL operation sequence, followed by compaction cycle, and any pause for demo querying of DB, could be
    # driven in potentially either of two ways:
    # 1. the for-loop below
    # 2. (ideally) Apache Airflow, as originally envisioned

    if TESTING and TESTING_CSV_LIMIT is not None:
        time_period_tables = [trip_files.head(min(len(trip_files), TESTING_CSV_LIMIT))]

    # separate trip_file entries into separate pandas DataFrames for each time period (year, for now) and store them
    # chronologically in time_period_tables list
    # simulate_time_period() assumes the filtration into time periods will retain the sort done above, which fortunately
    # groupby guarantees — see
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html#pandas-dataframe-groupby
    year_groups = trip_files.groupby(['Year'])
    time_period_tables = [year_groups.get_group(year) for year in year_groups.groups]

    # code left from previous hacks in case useful for future ones:
    # # time_period_tables = [trip_files]
    # # stopgap measure to get data into database with geomesa's S3 ingestion not working and without doubling HDFS
    # # taking them 3 at a time to at least allow all three spark workers to be used
    # step = 12
    # # obviously not good Pandas usage, but I need to iterate over the rows as DataFrames
    # time_period_tables = [trip_files[i:i+step] for i in range(0, len(trip_files)-1, step)]

    [simulate_time_period(time_period_table, config) for time_period_table in time_period_tables]


if __name__ == '__main__':
    main()
