#!/usr/bin/env python3
# coding=utf-8
#  Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

# std lib
from os import path
from typing import List, Dict, Union, Tuple
from subprocess import run, PIPE

# boto3
import boto3
import botocore
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient

# pandas
from pandas import DataFrame

# predictrip
from common import load_config, get_s3_client, get_s3_resource, get_s3_bucket, \
    build_structfield_for_column, build_structtype_for_file, compress_string, \
    PREDICTRIP_REPO_ROOT, SPARK_DATATYPE_FOR_LATLONG, ATTRIBUTES_FOR_COL, UNDESIRED_COLUMNS, \
    TRIPFILE_METADATA_COLS, TRIPFILE_METADATA_KEY_COL_IDX, TRIPFILE_METADATA_FILENAME_COL_IDX

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
    # TODO: the ordering of the following must stay consistent with TRIPFILE_METADATA_COLS. do that programmatically
    return [key, fn, kind, int(year), int(month)]


def clean_and_join_time_period_data(time_period_metadata: DataFrame) -> None:
    # TODO: add docstring
    # submit first spark job (Python) to spark cluster

    # NOTE: for compatibility with pyspark method used on the other end, it MUST be that orient='records' and lines=True
    table_json = time_period_metadata.to_json(orient='records', lines=True)
    # to avoid python/OS errors about argument length, compress
    table_compressed = compress_string(table_json)

    # construct call to spark-submit
    spark_submit = path.join(config['spark_home'], 'bin', 'spark-submit')
    # don't need to specify to spark-submit anything set in spark-defaults.conf or SparkConf
    script = path.join(PREDICTRIP_REPO_ROOT, 'code', 'python', 'clean_and_join.py')
    cmd = [spark_submit, script, table_compressed]
    # Note: don't expand cmd list in run call. including PREDICTRIP_REPO_ROOT part of path in script is redundant of
    # using cwd arg, but *shrug* this way more resilient to future code changes
    comp_proc = run(cmd, check=True, cwd=PREDICTRIP_REPO_ROOT)
    print(comp_proc.stdout)


def cast_and_insert_time_period_data(time_period_metadata: DataFrame) -> None:
    # TODO: add docstring
    # submit second spark job (Java) to spark cluster

    # the Java code run by that job should:
    # use SQL statements to create point columns and cast timestamp columns using GeoMesa Spark JTS datatype-casting UDFs as appropriate
    # ref https://www.geomesa.org/documentation/user/spark/sparksql_functions.html#st-point
    # SELECT st_point(Pickup_Longitude, Pickup_Latitude) AS Pickup_Point

    # call DataFrame's .save() method provided by GeoMesa

    pass


if __name__ == '__main__':

    config = load_config()

    # TODO: break down into more functions?

    # TODO: account for info in
    #  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-multiprocessing
    boto_session = boto3.session.Session()
    s3_client = get_s3_client(boto_session)
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
    # would be nice to declare datatype of each column, but constructor only accepts a singleton dtype argument
    trip_files = [parse_key(obj.key) for obj in objs]
    trip_files = DataFrame(trip_files, columns=[col[0] for col in TRIPFILE_METADATA_COLS])

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

    # doing simplest implementation for now: one chunk (i.e. no periodic compaction cycle)
    # otherwise, at this point we would separate trip_files entries into separate pandas DataFrames for each time period
    # and store them chronologically in time_period_tables list
    time_period_tables = [trip_files]

    for time_period_table in time_period_tables:
        # assuming any filtration into time periods will have preserved their internal sort
        print('Time period: {first.Month}/{first.Year} – {last.Month}/{last.Year}'
              .format(first=time_period_table.iloc[0], last=time_period_table.iloc[-1]))

        print('Cleaning and joining time period data')
        clean_and_join_time_period_data(time_period_table)

        # TODO: need to change first path segment of key to 'intermediate' and strip '.csv' extensions from filenames
        #  and keys so that Spark's read to DataFrame finds the parquet files

        # print('Casting and inserting time period data')
        # cast_and_insert_time_period_data(time_period_table)

        # TODO (future): trigger compaction cycle
