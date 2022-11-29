# Data Pipelines

by Vicente Lizana

## Project Summary

The need for data pipelines has brought Apache Airflow into the Sparkify toolbox,
the idea of this project is the further automatization of the ETL process for the
Data Warehouse in AWS Redshift by using data pipelines.

The pipelines consist of the following stages:
- Staging: Building the staging tables (song and events) from json data stored in
S3 buckets.
- Fact table creation: Build the songplays Fact Table in the Star Schema by
combining the staging tables.
- Dimension tables creation: Build the rest of the tables (users, songs, artists
and time) from the staging tables data.
- Data quality check: Verify that the data was correctly loaded in the analytical
tables.

The pipeline is run every hour, keeping the data warehouse up to date to be used
by the analytics team.

## How to run the project

### Prerequisites

- Running Airflow instance: The DAG and plugin are to be installed in an Apache
Airflow instance (version 1.X, tested in 1.10.2)

- AWS Redshift data warehouse: An instance of a Redshift cluster must be available
with a security group that allows connecting from the Airflow instance. Credentials
for a Redshift user with the right permissions must be added to Airflow in a
Postgres Hook with the `redshift` id.

- AWS programatic access credentials: An AWS Hook must be configured in Airflow
with the `aws_credentials` id in order to access S3 buckets.

### Table creation

The SQL queries to create the tables (`create_tables.sql`) must be run on the
Redshift cluster if the tables do not exist yet. Redshift's query editor can be
used for this purpose.

Once the tables are created, the DAG can be turned on in Airflow's interface and
the pipeline will execute every hour.

## Files on the project:

- `create_tables.sql`: Queries for table creation.
- `dags`:
  - `sparkify_dag.py`: Main DAG describing the pipeline for the project.
- `plugins`:
  - `helpers`:
    - `sql_queries.py`: Helper class containing SQL queries to aid in the
    loading stage, specifically containing the `SELECT` statements for
    each analytical table.
  - `operators`:
    - `stage_redshift.py`: Operator to load json data from S3 buckets into
    Redshift staging tables.
    - `load_fact.py`: Operator to transform the staging data into the fact
    table `songplays`.
    - `load_dimension.py`: Operator to fill the different dimension tables
    from the data in the staging tables.
    - `data_quality.py`: Operator to perform data quality checks into the
    analytical tables to verify the ETL process was sucessfully ran.
  
