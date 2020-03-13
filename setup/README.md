In here go scripts that:
- set up the VPC, using either a provisioning tool (e.g. Terraform) or aws-cli, intelligently use imaging to minimize redundant setup
    - in each EC2 instance:
        - set up the machine
            - set hostname
            - set timezone
            - add repos
                - AdoptOpenJDK
        - install the necessary software
        - `git clone` this repo and create sym links to ../config/ files as appropriate

For now, scp ~/.aws/credentials to each sparkworker. Debug their failure to get them from master later.

on each host in VPC:
`git clone` repo as ~/predictrip on each host

then, on the hosts indicated, link to the configuration files with the following commands:

all:
`PREDICTRIP_CONF=~/predictrip/config;
ln -sfT $PREDICTRIP_CONF/profile ~/.profile`

geomesa, hdfsnameN, hbaseN, sparkworkerN:
`PREDICTRIP_CONF=~/predictrip/config;
HADOOP_CONF=/usr/local/hadoop/etc;
ln -sfT $PREDICTRIP_CONF/hadoop/core-site.xml $HADOOP_CONF/hadoop/core-site.xml;
ln -sfT $PREDICTRIP_CONF/hadoop/hdfs-site.xml $HADOOP_CONF/hadoop/hdfs-site.xml;
ln -sfT $PREDICTRIP_CONF/hadoop/hadoop-env.sh $HADOOP_CONF/hadoop/hadoop-env.sh;
ln -sfT $PREDICTRIP_CONF/hadoop/masters $HADOOP_CONF/hadoop/masters;
ln -sfT $PREDICTRIP_CONF/hadoop/yarn-site.xml $HADOOP_CONF/hadoop/yarn-site.xml;
ln -sfT $PREDICTRIP_CONF/hadoop/yarn-env.sh $HADOOP_CONF/hadoop/yarn-env.sh`

geomesa, hbaseN:
`PREDICTRIP_CONF=~/predictrip/config;
HBASE_CONF=/usr/local/hbase/conf;
ln -sfT $PREDICTRIP_CONF/hbase/hbase-site.xml $HBASE_CONF/hbase-site.xml;
ln -sfT $PREDICTRIP_CONF/hbase/hbase-env.sh $HBASE_CONF/hbase-env.sh;
ln -sfT $PREDICTRIP_CONF/hbase/backup-masters $HBASE_CONF/backup-masters;
ln -sfT $PREDICTRIP_CONF/hbase/regionservers $HBASE_CONF/regionservers`

geomesa:
`PREDICTRIP_CONF=~/predictrip/config;
GEOMESA_CONF=/usr/local/geomesa-hbase/conf;
GEOMESA_BIN=/usr/local/geomesa-hbase/bin;
HBASE_CONF=/usr/local/hbase/conf;
GEOSERVER_HOME=/usr/local/geoserver;
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/geomesa-site.xml $GEOMESA_CONF/geomesa-site.xml;
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/geomesa-env.sh $GEOMESA_CONF/geomesa-env.sh;
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/application.conf $GEOMESA_CONF/conf/application.conf;
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/install-hadoop.sh $GEOMESA_BIN/install-hadoop.sh;
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/install-hbase.sh $GEOMESA_BIN/install-hbase.sh;
ln -sfT $HBASE_CONF/hbase-site.xml $GEOSERVER_HOME/webapps/geoserver/WEB-INF/classes/hbase-site.xml`

spark*, geomesa:
`PREDICTRIP_CONF=~/predictrip/config;
SPARK_CONF=/usr/local/spark/conf;
ln -sfT $PREDICTRIP_CONF/spark/spark-env.sh $SPARK_CONF/spark-env.sh;
ln -sfT $PREDICTRIP_CONF/spark/spark-defaults.conf $SPARK_CONF/spark-defaults.conf;
ln -sfT $PREDICTRIP_CONF/spark/metrics.properties $SPARK_CONF/metrics.properties`


hacky workaround to get hadoop to use different sets of workers for YARN and HDFS:
sparkworkersN, geomesa:
`PREDICTRIP_CONF=~/predictrip/config;
HADOOP_CONF=/usr/local/hadoop/etc;
ln -sfT $PREDICTRIP_CONF/hadoop/workers-yarn $HADOOP_CONF/hadoop/slaves`

hdfsnameN, hbaseN:
`PREDICTRIP_CONF=~/predictrip/config;
HADOOP_CONF=/usr/local/hadoop/etc;
ln -sfT $PREDICTRIP_CONF/hadoop/workers-dfs $HADOOP_CONF/hadoop/slaves`



geoserver installation:
make change to geomesa-hbase/bin/common-functions.sh described in
https://github.com/locationtech/geomesa/commit/e4d1bc5c9eec6ac091ac3649261dc240c516e943
before running
`/usr/local/geomesa-hbase/bin/install-shaded-hbase-hadoop.sh /usr/local/geoserver/webapps/geoserver/WEB-INF/lib/`

---
Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.
