[![Build Status](https://travis-ci.org/vrajat/mart.svg?branch=master)](https://travis-ci.org/vrajat/mart)
[![codecov](https://codecov.io/gh/vrajat/mart/branch/master/graph/badge.svg)](https://codecov.io/gh/vrajat/mart)

# Mart

Mart is a Java Service to measure, analyze, recommend and tune your application stack.
Technologies supported:

* Redshift

## Install

* Download the latest RPM from [Releases](https://github.com/vrajat/mart/releases) 
  to an AWS EC2 machine.
  
* Follow these steps: 


    # Install the RPM
    yum install -y inviscid-mart-*.rpm
    
    #Copy the sample configuration file
    cp /etc/mart/mart-sample.yml /etc/mart/sample.yml
    
 
 * Edit the configuration file. Add DB access info. Refer to [modules](#Modules)
 
 ## Modules
 ### QueryStatsCron
_QueryStatsCron_ captures query stats every 60 minutes, aggregates key metrics and stores
in a MySQL-compatible database. 

#### Configuration
|Name|Default|Description|  
|----|-------|-----------|
| frequencyMin|60|Interval in no. of minutes|
|redshift.url|<Empty>|URL of the Redshift Database|
|redshift.user|<Empty>|A admin user|
|redshift.password|<Empty>|Password|
|mysql.url|<Empty>|URL of the Redshift Database|
|mysql.user|<Empty>|A admin user|
|mysql.password|<Empty>|Password|

#### Statistics
Statistics is available in _query_stats_ table in MySQL database. It contains
statistics on the duration of queries aggregated by
* Database
* User
* Label
* Day

|Name|Description|
|----|-----------|
|db| Database connected to|
|user| User who submitted the query|
|query_group| Label of the queue (ref: [Redshift WLM] (https://docs.aws.amazon.com/redshift/latest/dg/c_workload_mngmt_classification.html)|
|day| Day on which query was submitted _YYYY-MM-DD_|
|min_duration| Minimum duration|
|avg_duration| Average duration|
|median_duration| Median duration|
|p75_duration| 75th Percentile of duration|
|p90_duration| 90th Percentile of duration|
|p95_duration| 95th Percentile of duration|
|p99_duration| 99th Percentile of duration|
|p999_duration| 999th Percentile of duration|
|max_duration| Max. duration|
 
