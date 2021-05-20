# Sparkify Airflow ETL

## Project Overiew

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring me into the project and expect me to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Database structure overview

![ER Diagram](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1607614393/Song_ERD.png)
*From Udacity*

## How to run

- Start by cloning this repository
- Install all python requirements from the requirements.txt
- Spin up your own Airflow instance and move the files in this repository into their appropriate places