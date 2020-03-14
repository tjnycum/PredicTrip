For the code that actually drives the pipeline after its (virtual) hardware and software have been configured.

Taking an inclusion list approach to specifying the columns of the input that are retained in the output should make the
code more maintainable as it means the code only needs to be updated when reporting changes by the TLC alter the set of 
columns that _are_ of interest, rather than the (much larger) set _not_ of interest.

# Notes

Notes relocated from source code, pending potential clean up and integration here:

## Alternative Spark approach using RDDs

Spark DataFrames and RDDs cannot be created in code running on workers, because the SparkContext object cannot be 
serialized and transmitted to them. Attempting to do so causes an exception—see 
https://issues.apache.org/jira/browse/SPARK-5063.

Given that, I can think of only a few ways to do the downloading of the heterogeneously structured CSVs inside a
mapped function:
1. Use pandas to still be able to homogenize the structure relatively easily by working with the columns by name.
Pandas will read from S3, but will no doubt download the whole file first. (Either that or we'd have to micromanage 
pandas DF creation for chunks of the file at a time, if pandas allows that.) In that case all the cleaning and 
structural homogenization might as well be done in that same mapped function while they're already pandas DFs.

2. If pyspark can still read from S3 without referencing the SparkContext (which I'm skeptical of), then the lines
of the CSVs could be processed progressively as they're downloaded. If built-in Spark methods of reading from
S3 could not be used, then we'd have to use Boto3. Beginning the processing before the whole files were
downloaded would likely require multi-threading and some File-like wrapper objects to prevent the processing from
overrunning the download's progress. In any case, the parsing of the CSVs would probably have to be done with the csv
module. The Iterable-returning method I found before was limited to returning DictReader objects, which output tuples
of column name and value for each row. I'd need to make the handling vary depending on the metadata about the
CSV. And since it'd already be visiting each "cell" of the CSV, it might as well do the structural
homogenization in the same pass through the data. In my tentative approach to this before, I had the mapped
function dynamically choosing another function to call (based on which CSV it was) that would iterate over the
CSV rows doing whatever hard-coded set of manipulations was appropriate for its batch of files. (That way we'd
avoid lots of conditional statements being evaluated redundantly for each row of a file, within which the
evaluations would always be the same anyway.) A more ambitious alternative might be to dynamically create a lambda or 
something. Any future joining should be built into the same single pass over each row. The logic of that one pass seems
more daunting than #3 below.

3. flatMap two functions, load_csv_file and standardize_csv_row. In the first, download the CSV for the file 
(probably having to use Boto3) and return a list of its rows, each in a tuple with a sidecar of whatever metadata would 
be needed by the second, which would parse the CSV of the line and homogenize its structure, using the same sort of 
dynamically selected or constructed functions as in #2. After those two functions have been mapped, construct a DF with
a newly consistent schema and output it to/for the DB. In between the mapped function calls, we would want to ensure it
did not do any shuffling of the partitions. With shuffling, multiple mapped functions would mean unnecessary
transfer within the cluster. Joining would be done row-wise within standardize_csv_row using a dictionary.

Of the above options, #3 seems best.
Note: S3 client objects would need to be created within any mapped function downloading from S3 with Boto —
See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-multiprocessing)

## Current database table ("catalog") schema

In the interest of minimizing record size, it doesn't prepare for summarization, which would involve adding a new
integer attribute (column) Trip_Count and changing Passenger_Count to float.

---
Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.
