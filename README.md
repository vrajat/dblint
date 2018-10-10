[![Build Status](https://travis-ci.org/vrajat/mart.svg?branch=master)](https://travis-ci.org/vrajat/mart)
[![codecov](https://codecov.io/gh/vrajat/mart/branch/master/graph/badge.svg)](https://codecov.io/gh/vrajat/mart)

# Mart

Mart is a Java Service to measure, analyze, recommend and tune your application stack.
Technologies supported:

* Redshift

## Install
Download the latest RPM from [Releases](https://github.com/vrajat/mart/releases) to an AWS EC2 machine.
  


    # Install the RPM
    yum install -y inviscid-mart-*.rpm
    
    #Copy the sample configuration file
    cp /etc/mart/mart-sample.yml /etc/mart/sample.yml
    
    # Edit the configuration file. 
    # Add DB access info. 
    # Refer to modules

    # Start the service
    /etc/init.d/mart.service start
    
    # Check & monitor healthchecks
    curl http://localhost:8081/healthchecks?pretty=true
    
    # Monitor logs
    tail -f /var/log/mart/server.log
    tail -f /var/log/mart/request.log
    
    View metrics
    ls /var/log/mart/metrics/
    

## Configuration

    redshift:
    # URL of the Redshift Database. SSL not supported. Set `?ssl=false`|
      url: ""
    # An admin user with access to all of Redshift system tables
      user: ""
    # Password of admin user
      password: ""
    # MySQL database where metrics will be stored.
    mySql:
    # URL of the MySQL Database. SSL not supprt. Set `useSSL=false`|
      url: ""
      user: ""
      password: ""

## REST
_/redshift/high_cpu_capture_

Run one instance of _ConnectionsCron_ to capture all the statistics


## Modules
### QueryStatsCron
_QueryStatsCron_ captures sql stats every 60 minutes, aggregates key metrics and stores
in a MySQL-compatible database. 

#### Statistics
Statistics is available in _query_stats_ table in MySQL database. It contains
statistics on the duration of queries aggregated by
* Database
* User
* Label
* Timestamp normalized to Hour

|Name|Description|
|----|-----------|
|`db`| Database connected to|
|`user`| User who submitted the sql|
|`query_group`| Label of the queue (ref: [Redshift WLM](https://docs.aws.amazon.com/redshift/latest/dg/c_workload_mngmt_classification.html))|
|`timestamp_hour`| Timestamp normalized to hour on which sql was submitted _YYYY-MM-DD HH:00:00_|
|`min_duration`| Minimum duration|
|`avg_duration`| Average duration|
|`median_duration`| Median duration|
|`p75_duration`| 75th Percentile of duration|
|`p90_duration`| 90th Percentile of duration|
|`p95_duration`| 95th Percentile of duration|
|`p99_duration`| 99th Percentile of duration|
|`p999_duration`| 999th Percentile of duration|
|`max_duration`| Max. duration|

### BadQueriesCron
_BadQueriesCron_ parses all queries every 60 seconds and finds queries which are sub-optimal. 
Queries are checked for the following anti-patterns:
- Large no. of joins: No. of joins is > 10

#### Schema
Information on bad queries is available in _bad_user_queries_ table in MySQL database. It
stores the following information for every query:

|Name|Description|
|----|-----------|
|`query_id`|Query ID assigned by Redshift|
|`user_id`| User ID who submitted the query|
|`transaction_id`| Transaction ID of the query|
|`pid`| ID of the process that executed the query|
|`start_time`| Query start time|
|`end_time`| Query end time|
|`duration`| Query end time - start time|
|`db`| DB name where query was run|
|`aborted`| Whether query was aborted|
|`query`| Query text|

### ConnectionsCron
_ConnectionsCron_ takes a snapshot of live connections.

### Schema
Information is stored _user_connections_. 

|Name|Description|
|----|-----------|
|`poll_time`|Time at which data was polled|
|`start_time`| Connection start time|
|`process`| Process ID associated with connection|
|`user_name`| Owner of the connection|
|`remote_host`| Origin host name of the connection|
|`remote_port`| Origin port of the connection|
