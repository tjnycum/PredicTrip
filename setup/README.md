# Setup

TODO: Use a provisioning tool for as much of this as possible and script the rest

## Create VPC

| Hostname | Instance type |
|------------------|---------------|
| gateway | t2.micro |
| geomesa | t3.small |
| hbase{1-3} | t3.medium |
| hdfsname{1-2} | t3.medium |
| sparkmaster | t3.medium |
| sparkworker{1-3} | t3.small |

TODO: more details

## Configure hosts
Hosts: all

### Name
Note: you must replace `{hostname}` in the following.
```shell script
sudo hostnamectl set-hostname {hostname}
sudo sed -i 's/preserve_hostname: false/preserve_hostname: true' /etc/cloud/cloud.cfg
```

### Time zone (optional)
Note: you must replace `{time_zone}` in the following (with, e.g., `Americas/Los_Angeles`).
```shell script
sudo timedatectl set-timezone {time_zone}
```

### APT repositories
```shell script
sudo add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/
wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -
```

### /etc/hosts
Unfortunately AWS VPCs don't support multicast addressing. Otherwise we could just install zeroconf on the hosts and be 
done with it.

So, for now, hard-code them: Put in place on all hosts an `/etc/hosts` file mapping all the hosts' names to their private IP addresses.

### SSH keys
Generate a key pair for one of the hosts (with the default output file names). Add the public key to that host's 
`~/.ssh/authorized_keys`. Copy the key pair and the `authorized_keys` file to all the other hosts.

## Install software packages

### Java
Hosts: all
```shell script
sudo apt install adoptopenjdk-8-jdk-hotspot
```

### Hadoop
Hosts: hdfsname*, hbase*, geomesa, sparkworker*
```shell script
HADOOP_VER=2.7.7
wget https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz
tar xzf hadoop-$HADOOP_VER.tar.gz
sudo mv hadoop-$HADOOP_VER /usr/local/
sudo ln -sT hadoop-$HADOOP_VER /usr/local/hadoop
sudo mkdir /var/log/hadoop /var/local/hadoop
sudo chown ubuntu:ubuntu /var/log/hadoop /var/local/hadoop
```

### HBase
Hosts: hdfsname*, hbase*
```shell script
HBASE_VER=1.4.12
wget http://apache.osuosl.org/hbase/hbase-$HBASE_VER/hbase-$HBASE_VER-bin.tar.gz
tar xzf hbase-$HBASE_VER-bin.tar.gz
sudo mv hbase-$HBASE_VER /usr/local/
sudo ln -sT hbase-$HBASE_VER /usr/local/hbase
sudo mkdir /var/log/hbase /var/local/hbase
sudo chown ubuntu:ubuntu /var/log/hbase /var/local/hbase
```

### Spark
Hosts: sparkworker*

TODO

### GeoMesa
Host: geomesa

TODO

Fix `$GEOMESA_HBASE_HOME/bin/common-functions.sh` as described in
[this git commit](https://github.com/locationtech/geomesa/commit/e4d1bc5c9eec6ac091ac3649261dc240c516e943):
```shell script
pushd $GEOMESA_HBASE_HOME/..
curl -L --output common-functions.patch https://github.com/locationtech/geomesa/commit/e4d1bc5c9eec6ac091ac3649261dc240c516e943.patch
sed -i 's/geomesa-tools\/bin\/common-functions.sh/geomesa-hbase\/bin\/common-functions.sh/g' common-functions.patch
if git apply common-functions.patch; then
  echo Patch succeeded
  rm common-functions.patch
  popd
else
  echo Patch failed
fi
```

### GeoServer
Host: geomesa

#### Core
```shell script
GEOSERVER_DATA_DIR=/var/local/geoserver
GEOSERVER_VER=2.15.5
curl -L --output geoserver-$GEOSERVER_VER-bin.zip https://sourceforge.net/projects/geoserver/files/GeoServer/$GEOSERVER_VER/geoserver-$GEOSERVER_VER-bin.zip/download
unzip geoserver-$GEOSERVER_VER-bin.zip
if [ -d "$GEOSERVER_DATA_DIR" ]; then
    rm -r geoserver-$GEOSERVER_VER/data_dir
else
    sudo mv geoserver-$GEOSERVER_VER/data_dir $GEOSERVER_DATA_DIR
fi
sudo mv geoserver-$GEOSERVER_VER /usr/local/
sudo ln -sfT /usr/local/geoserver-$GEOSERVER_VER /usr/local/geoserver
```

#### Plugins
```shell script
GEOMESA_VER=2.4.0
GEOSERVER_VER=2.15.5
SCALA_VER=2.11
GEOMESA_HBASE_HOME=/usr/local/geomesa-hbase
GEOSERVER_HOME=/usr/local/geoserver

tar xf $GEOMESA_HBASE_HOME/dist/gs-plugins/geomesa-hbase-gs-plugin_$SCALA_VER-$GEOMESA_VER-install.tar.gz --directory $GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib/
install-shaded-hbase-hadoop.sh $GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib/

ln -sfT $GEOMESA_HBASE_HOME/dist/gs-plugins/geomesa-process-wps_$SCALA_VER-$GEOMESA_VER.jar $GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib/geomesa-process-wps_$SCALA_VER-$GEOMESA_VER.jar
curl -L --output geoserver-$GEOSERVER_VER-wps-plugin.zip https://sourceforge.net/projects/geoserver/files/GeoServer/$GEOSERVER_VER/extensions/geoserver-$GEOSERVER_VER-wps-plugin.zip/download
unzip -d $GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib/ geoserver-$GEOSERVER_VER-wps-plugin.zip
curl -L --output geoserver-$GEOSERVER_VER-wps-cluster-hazelcast-plugin.zip https://sourceforge.net/projects/geoserver/files/GeoServer/$GEOSERVER_VER/extensions/geoserver-$GEOSERVER_VER-wps-cluster-hazelcast-plugin.zip/download
unzip -d $GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib/ geoserver-$GEOSERVER_VER-wps-cluster-hazelcast-plugin.zip
curl -L --output geoserver-$GEOSERVER_VER-monitor-plugin.zip https://sourceforge.net/projects/geoserver/files/GeoServer/$GEOSERVER_VER/extensions/geoserver-$GEOSERVER_VER-monitor-plugin.zip/download
unzip -d $GEOSERVER_HOME/webapps/geoserver/WEB-INF/lib/ geoserver-$GEOSERVER_VER-monitor-plugin.zip
```
References:
- GeoMesa User Manual:
    - [12.1. GeoMesa Processes: Installation](https://www.geomesa.org/documentation/user/process.html#installation)
    - [13.1.6. Installing GeoMesa HBase in GeoServer](https://www.geomesa.org/documentation/user/hbase/install.html#installing-geomesa-hbase-in-geoserver)
- GeoServer 2.15.x User Manual:
    - [Installing the Monitor Extension](https://docs.geoserver.org/2.15.x/en/user/extensions/monitoring/installation.html)


### Python modules
Hosts: sparkworker*
```shell script
GEOMESA_VER=2.4.0
pip3 install https://repo.eclipse.org/content/groups/geomesa/org/locationtech/geomesa/geomesa_pyspark/$GEOMESA_VER/geomesa_pyspark-GEOMESA_VER.tar.gz
```

## Clone repository
Hosts: all

`git clone` this repo as ~/predictrip

## Configure software packages

### Shells
Hosts: all
```shell script
PREDICTRIP_CONF=$PREDICTRIP_HOME/config
ln -sfT $PREDICTRIP_CONF/profile ~/.profile
ln -sfT $PREDICTRIP_CONF/bash_profile ~/.bash_profile
```
Re-login before continuing. The commands beyond rely on certain
variables having been set in the environment by those shell startup scripts.

### Hadoop
Hosts: hdfsname*, hbase*, sparkworker*, geomesa
```shell script
PREDICTRIP_CONF=$PREDICTRIP_HOME/config
HADOOP_CONF=$HADOOP_HOME/etc
ln -sfT $PREDICTRIP_CONF/hadoop/core-site.xml $HADOOP_CONF/hadoop/core-site.xml
ln -sfT $PREDICTRIP_CONF/hadoop/hdfs-site.xml $HADOOP_CONF/hadoop/hdfs-site.xml
ln -sfT $PREDICTRIP_CONF/hadoop/hadoop-env.sh $HADOOP_CONF/hadoop/hadoop-env.sh
ln -sfT $PREDICTRIP_CONF/hadoop/masters $HADOOP_CONF/hadoop/masters
ln -sfT $PREDICTRIP_CONF/hadoop/yarn-site.xml $HADOOP_CONF/hadoop/yarn-site.xml
ln -sfT $PREDICTRIP_CONF/hadoop/yarn-env.sh $HADOOP_CONF/hadoop/yarn-env.sh
```
Hosts: sparkworkersN, geomesa
```shell script
ln -sfT $PREDICTRIP_CONF/hadoop/workers-yarn $HADOOP_CONF/hadoop/slaves
```
Hosts: hdfsname*, hbase*
```shell script
ln -sfT $PREDICTRIP_CONF/hadoop/workers-dfs $HADOOP_CONF/hadoop/slaves
```

### HBase
Hosts: hdfsname*, hbase*, geomesa
```shell script
PREDICTRIP_CONF=$PREDICTRIP_HOME/config
HBASE_CONF=$HBASE_HOME/conf
ln -sfT $PREDICTRIP_CONF/hbase/hbase-site.xml $HBASE_CONF/hbase-site.xml
ln -sfT $PREDICTRIP_CONF/hbase/hbase-env.sh $HBASE_CONF/hbase-env.sh
ln -sfT $PREDICTRIP_CONF/hbase/backup-masters $HBASE_CONF/backup-masters
ln -sfT $PREDICTRIP_CONF/hbase/regionservers $HBASE_CONF/regionservers
```

### Spark
Hosts: sparkworker*, geomesa
```shell script
PREDICTRIP_CONF=$PREDICTRIP_HOME/config
SPARK_CONF=$SPARK_HOME/conf
ln -sfT $PREDICTRIP_CONF/spark/spark-env.sh $SPARK_CONF/spark-env.sh
ln -sfT $PREDICTRIP_CONF/spark/spark-defaults.conf $SPARK_CONF/spark-defaults.conf
ln -sfT $PREDICTRIP_CONF/spark/metrics.properties $SPARK_CONF/metrics.properties
```

### GeoMesa
Host: geomesa
```shell script
PREDICTRIP_CONF=$PREDICTRIP_HOME/config
GEOMESA_HBASE_CONF=$GEOMESA_HBASE_HOME/conf
GEOMESA_HBASE_BIN=$GEOMESA_HBASE_HOME/bin
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/geomesa-site.xml $GEOMESA_HBASE_CONF/geomesa-site.xml
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/geomesa-env.sh $GEOMESA_HBASE_CONF/geomesa-env.sh
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/application.conf $GEOMESA_HBASE_CONF/application.conf
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/install-hadoop.sh $GEOMESA_HBASE_BIN/install-hadoop.sh
ln -sfT $PREDICTRIP_CONF/geomesa-hbase/install-hbase.sh $GEOMESA_HBASE_BIN/install-hbase.sh
ln -sfT $PREDICTRIP_CONF/hbase/hbase-site.xml $GEOMESA_HBASE_CONF/hbase-site.xml
ln -sfT $PREDICTRIP_CONF/hbase/hbase-site.xml $GEOSERVER_HOME/webapps/geoserver/WEB-INF/classes/hbase-site.xml
```

### GeoServer
Host: geomesa

TBD

---
Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.
