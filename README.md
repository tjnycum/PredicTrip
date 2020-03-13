# About

Completed during my Insight Data Engineering Fellowship, Silicon Valley session 20A.

# Notes

Notes relocated from source code and/or config files, pending potential clean up and integration above:

## GeoMesa Feature IDs

GeoMesa only supports vertical partitioning of tables, referred to in the docs as "splitting", on the basis of 
feature ID.

The GM docs' general advice is to use an MD5 hash of the "whole record", which makes sense for ensuring feature IDs
are well distributed. It would also potentially allow duplicate insertions to be prevented, but GM doesn't actually
enforce uniqueness of the feature ID. It defaults, however, to using UUIDs in which the earlier digits are taken from
a Z-ordering of a timestamp and geometry, which is useful if you want to keep features that are clustered in time
and/or space also clustered in feature-ID space. As feature ID is used as the first component of the HBase key,
clustered feature IDs should be more likely to end up in the same database shard. Our typical queries will aggregate
features that are clustered in time _relative to the week_, though, so for the time component, in place of the real
full Pickup timestamp, we'll use a "fake" one constructed from just the number of seconds from the start of the week
in which the real timestamp falls. In effect we wrap the actual timestamps around the first week of January 1970. One
of the parameters of the Z3 calculation is the interval at which time is broken up. As all the events happen
within the first week of January 1970 (start of the Unix epoch) as far as this UUID generation is concerned, we
chose the shortest option, day, so that time still gets broken up significantly. Alternatively, week would lead to one
break (assuming the actual calendar weeks are observed rather than the timestamps simply divided by the number of
seconds in a canonical week), and month or year would lead to none.
    
See https://www.geomesa.org/documentation/user/datastores/runtime_config.html#geomesa-feature-id-generator
    

## Timestamps
Specifically, interpretation of the ones in the input CSV files and how I store them thereafter

The timezone of the timestamps in the input CSVs isn't specified, but is presumably NYC local time. We could store
them as UTC as one might in a system serving the whole globe, but I'm choosing not to for a couple reasons:
1) it would complicate the querying (whether manual or via a UI), and 
2) it would de-adjust for DST, which would obscure travel patterns, as U.S. residents (e.g. New Yorkers) generally
behave according to DST-adjusted times where it is legally observed. For this reason, even in a system serving the 
whole globe, you would probably want to index features using observed local time, even if they weren't actually 
stored (only) that way. (Trips potentially spanning time zone and/or DST boundaries (spatial or temporal) would have to
be considered.)

## Numeric representation of day of the week
   
{Mon, …, Sun} → {0, …, 6}, in accordance with:
1. the behavior of Spark SQL's date_trunc built-in function,
2. international standard (ISO 8601) re semantics of first day of week, and
3. the 0-based indexing that's more common among programming languages, including Python, and more convenient in
      calculations.

## Z3 indices

The temporal interval of any Z3 index should strike a compromise between the "by nothing" (i.e. no sharding) that
would be best for the usual queries from the front-end, which aren't temporally bounded (except within week) and what
would be best for the queries involved in the maintenance cycle, which would want to find a day's or week's worth of
trips for summarization or deletion (depending on age). With a caching layer implemented, monthly might strike a
better balance, but for now, with no (separate) caching layer, we might want to choose yearly to prioritize
performance of the much more frequent reads from the front-end.

## Potential use of column groups

The extent to which GeoMesa supports the use of column groups (CGs) with the HBase datastore is unclear. The 
documentation says they are supported. Indeed when I tried using them in the trip feature schema, thinking they 
might improve read performance, GM did not complain. Their use did roughly double the time taken to ingest features
from intermediate files, and (to a lesser extent) increased DB storage consumption. My attempted read queries also
failed to return. So when one of the maintainers said informally that GM doesn't support CGs because they massively
slow things down, I simply removed them, without bothering to ask for clarification of the exact extent of the 
support.

Potential column group set:
- f: columns needing to be read to evaluate filter
- s: columns needing to be read only for those features that pass filter
- i: columns already included in indices, so often unnecessary to read at all
 
---
Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.
