Employee Data Management Pipeline
Overview
This project implements a robust data pipeline for managing employee records using AWS Glue, S3, and Amazon Redshift. 
It utilizes a Slowly Changing Dimension (SCD) Type 2 approach to maintain historical data, ensuring that all changes to employee information are tracked effectively.

Key Features
Data Ingestion:
Load employee data from CSV files stored in Amazon S3.
Perform data validation to ensure the correct format and structure.

Data Transformation:
Clean and standardize incoming data by renaming columns, converting data types, and adding calculated fields such as hashes and flags.
Implement a schema that includes fields for handling effective dates and versioning.

Data Storage:
Store transformed data in a staging table within Amazon Redshift.
Utilize a temporary table for managing the upsert logic during data updates.

Historical Tracking:
Implement a stored procedure to manage the SCD Type 2 pattern, allowing for updates and historical tracking of employee records.
Automatically deactivate outdated records and maintain a history of changes.

Job Orchestration:
Utilize AWS Glue for job scheduling and orchestration, ensuring seamless execution of the data pipeline.

Technologies Used
AWS Glue: For ETL (Extract, Transform, Load) operations.
Amazon S3: For data storage.
Amazon Redshift: For data warehousing.
Python and PySpark: For data manipulation and transformations.
PostgreSQL: For executing stored procedures within Redshift.

Getting Started
Set Up AWS Environment:
Configure IAM roles with appropriate permissions.
Create necessary connections in AWS Glue for Redshift.

Data Ingestion:
Ingest CSV files into the specified S3 bucket.

Execute Glue Jobs:
Run the Glue jobs to ingest, transform, and load data into Redshift.

Run Stored Procedures:
Call the scd2_emp stored procedure to manage historical data updates.

Future Improvements
Implement more sophisticated error handling and logging mechanisms.
Enhance data validation checks during the ingestion process.
Explore additional data sources and integrate them into the pipeline.
