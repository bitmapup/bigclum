
# Bigclum

# Before running this program. You need:

* Install Big Data cluster (hdfs, mrjob and pyspark)
* Install Ganglia
* Install Collectd
* Install Nagios

# This repository contains:

* codes: folder where the Spark code, Hadoop code, and dataset are.
* templates: folder where the html file is.
* static: folder where the images are saved.
* ip: ip of the master node. To know ip use the command "ifconfig".
* bigclum.py: It's a python script where Hadoop code and Spark code are executed in the cluster and monitoring data collection.
* config.cfg: configuration file containing.
   -inputHadoopPath: Hadoop code path.
   -inputSparkPath: Spark code path.
   -nameapplication: application name and path where collected data, by the monitoring tool, is saved.
   -dataset: filename of the dataset stored in HDFS. We suppose the file containing the dataset is already in HDFS.
   -cluster: cluster hostnames. Put first the hostname of the master node and then the hosts of the remaining nodes. To know node hostname use the command: "cat /etc/hostname".
   -metrics: set of metrics to gather from the monitoring tools.

# To run

python bigclum.py config.cfg