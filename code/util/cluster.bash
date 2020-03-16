#!/usr/bin/env bash
# Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

USE_YARN=1

# FIXME: ssh -t bypasses /etc/hosts file?
# workround:
# https://unix.stackexchange.com/questions/20784/how-can-i-resolve-a-hostname-to-an-ip-address-in-a-bash-script#comment551871_20793

function get_host_ip () {
  # TODO: add error detection and handling
  getent hosts "$1" | awk '{ print $1 }'
}

HDFS_NN=$(get_host_ip hdfsname1)
HBASE_MASTER=$(get_host_ip hdfsname1)
YARN_AM=$(get_host_ip geomesa)
SPARK_MASTER=$(get_host_ip sparkmaster)

function start-cluster () {

  echo "Starting HDFS"
  ssh -t "$HDFS_NN" '/usr/local/hadoop/sbin/start-dfs.sh' || exit

  if ((USE_YARN)); then
    echo "Starting YARN"
    ssh -t "$YARN_AM" '/usr/local/hadoop/sbin/start-yarn.sh' || exit
  else
    echo "Starting Spark"
    ssh -t "$SPARK_MASTER" '/usr/local/spark/sbin/start-all.sh' || exit
  fi

  echo "Starting HBase"
  ssh -t "$HBASE_MASTER" '/usr/local/hbase/bin/start-hbase.sh'

}

function stop-cluster () {

  echo "Stopping HBase"
  ssh -t "$HBASE_MASTER" '/usr/local/hbase/bin/stop-hbase.sh'

  if ((USE_YARN)); then
    echo "Stopping YARN"
    ssh -t "$YARN_AM" '/usr/local/hadoop/sbin/stop-yarn.sh'
  else
    echo "Stopping Spark"
    ssh -t "$SPARK_MASTER" '/usr/local/spark/sbin/stop-all.sh'
  fi

  echo "Stopping HDFS"
  ssh -t "$HDFS_NN" '/usr/local/hadoop/sbin/stop-dfs.sh'

}
