# Project: Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The main purpose is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. 

## Project Description

In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Build schema for Song Play Analysis
**Fact Table**

*songplays* records in log data associated with song plays
    
**Dimension Tables**

*users* in the app

*songs* in music database

*artists* in music database

*time* - timestamps of records in songplays broken down into specific units

## Project Design

The project includes the following 5 files:

*create_tables.py* - is where you'll create your fact and dimension tables for the star schema in Redshift.

*etl.py* - is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

*sql_queries.py* - is where you'll define you SQL statements, which will be imported into the two other files above.

*dwh.cfg*: - containing the CLUSTER, IAM_ROLE, S3 information.

*README.md* - provides project description.

## Project Steps

*Step 1*:

**Create Tables**:

* Design schemas for your FACT and DIMENSION tables.
    
* Write a SQL CREATE statement for each of these tables in *sql_queries.py*.
    
* Make sure the logic in *create_tables.py* to connect to the database and create these tables.
    
* Write SQL DROP statements to drop tables in the beginning of *create_tables.py* if the tables already exist.

* Launch a redshift cluster and create an IAM role that has read access to S3.

* Add redshift database and IAM role info to *dwh.cfg*.

* Test by running *create_tables.py* and checking the table schemas in your redshift database.

*Step 2*:

**Build ETL Pipeline**:

* Implement the logic in *etl.py* to load data from **S3** to **staging tables** on Redshift.

* Implement the logic in *etl.py* to load data from **staging tables** to **analytics tables** on Redshift.

* Test by running *etl.py* after running *create_tables.py* and running the analytic queries on *test.ipynb*.

* Delete your redshift cluster when finished.


