#!/bin/bash

#
# Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This script will attempt to install the client dependencies for HBase
# into a given directory. Usually this is used to install the deps into either the
# geomesa tools lib dir or the WEB-INF/lib dir of geoserver.

hbase_version="1.4.12"

# Load common functions and setup
if [ -z "${GEOMESA_HBASE_HOME}" ]; then
  export GEOMESA_HBASE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. $GEOMESA_HBASE_HOME/bin/common-functions.sh

install_dir="${1:-${GEOMESA_HBASE_HOME}/lib}"

# Resource download location
base_url="${GEOMESA_MAVEN_URL:-https://search.maven.org/remotecontent?filepath=}"

declare -a urls=(
  "${base_url}org/apache/hbase/hbase-common/${hbase_version}/hbase-common-${hbase_version}.jar"
  "${base_url}org/apache/hbase/hbase-protocol/${hbase_version}/hbase-protocol-${hbase_version}.jar"
  "${base_url}org/apache/hbase/hbase-client/${hbase_version}/hbase-client-${hbase_version}.jar"
  "${base_url}org/apache/hbase/hbase-server/${hbase_version}/hbase-server-${hbase_version}.jar"
  "${base_url}org/apache/hbase/hbase-hadoop-compat/${hbase_version}/hbase-hadoop-compat-${hbase_version}.jar"
  "${base_url}com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar"
)

downloadUrls "$install_dir" urls[@]
