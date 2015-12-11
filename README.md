# mapr-cluster-monitor
## Description

MapR Cluster Monitor is a python tool that collect node and cluster performance metrics from a mapr-cluster using the MapR REST API in realtime.
The tool  collects the performance stats for the cluster and the nodes in a csv file into a log.

Usage:

python cluster-monitor.py -u user -p password -m hostname

-u  ( user that has access to MapR Control System)
-p  ( password for the user)
-m  ( node hostname running the webserver )


The Performance logs are stored in the log Directory.




