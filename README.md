[![Build Status](https://travis-ci.org/dblintio/mart.svg?branch=master)](https://travis-ci.org/dblintio/mart)
[![codecov](https://codecov.io/gh/dblintio/mart/branch/master/graph/badge.svg)](https://codecov.io/gh/dblintio/mart)

# Mart

Mart is a Java Service to measure, analyze, recommend and tune your data engineering stack.
Technologies supported:

* Redshift
* MySQL
* PostgreSQL

Current features:

* Pretty Print SQL
* SQL Digest Query
* Analyze ETL scripts for Data Lineage and optimal concurrency
* Analyze MySQL slow query logs

Checkout a running version of mart at https://dblint.io

Please read [dblint.io: A toolset for database engineers](https://medium.com/@vrajat/dblint-io-a-toolset-for-database-engineers-2238bd1b9b37)
 for the vision and road map for mart.
 

## Development
Instructions to build and run on MacOS or Linux

### Build

    git clone https://github.com/dblintio/mart.git
    cd mart
    mvn test
    
### Run mart server on your laptop

    cd mart
    mvn clean package
    java -jar server/target/mart-server.jar server server/src/main/resources/dblint.yml
    
    # check port 8080
    # the call should return "healthy"
    curl -i http://localhost:8080
    
    #check port 8081 (admin port)
    # the call should return {"deadlocks":{"healthy":true}}
    curl -i http://localhost:8081/healtcheck
