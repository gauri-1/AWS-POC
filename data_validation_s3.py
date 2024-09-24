
#Initial Data Loading from S3

#Summary:
"""Purpose: This script is responsible for loading a CSV file from S3 into a Spark DataFrame and validating its schema.
Key Functions:
Logging: Set up to track the job's progress and any issues.
Parameter Handling: Retrieves S3 bucket and file name from command-line arguments.
CSV Reading: Reads the specified CSV file from S3 using Spark.
Schema Validation: Checks if the DataFrame's columns match predefined expected column names.
Data Presence Check: Validates that the DataFrame contains data."""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging 

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Spark and Glue Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['s3_target_path_key','s3_target_path_bucket'])
bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

# Default column names for validation
df_default_cols = ['EMPLOYEE_ID','FIRST_NAME','LAST_NAME','EMAIL','PHONE_NUMBER','HIRE_DATE','JOB_ID','SALARY','COMMISSION_PCT','MANAGER_ID','DEPARTMENT_ID']
    
try:
    if fileName.endswith('csv'):
        logging.info("The file is of format csv")
        
        s3_path = "s3://"+bucket+"/"+fileName
        print(s3_path)
        df = spark.read.option("header","True").csv(s3_path)
        
        col_names=df.columns
        
        if col_names==df_default_cols:
            logging.info("Column names are as expected")
        
        if df.count()>0:
            logging.info("The file contains data..")
            
except Exception as e:
    logging.info(e)
            
job.commit()
