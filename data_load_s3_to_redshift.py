#File 2: Loading Data into Redshift with Glue

"""Summary:
Purpose: This script processes multiple CSV files from a specified S3 folder, transforms the data, and loads it into a Redshift table.
Key Functions:
S3 Object Listing: Retrieves all CSV files in a given S3 folder.
Data Transformation:
Converts column names to lowercase.
Casts data types for certain columns (e.g., salary).
Dynamic Frame Creation: Converts Spark DataFrame to a Glue DynamicFrame for easier handling with Glue.
Redshift Loading: Loads the transformed data into a specified Redshift table after truncating it."""

import sys
import boto3
import io 
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Spark and Glue Contexts
args = getResolvedOptions(sys.argv, ['s3bucket','folder','file_path'])
bucket = args.get('s3bucket')
folder = args.get('folder')
file_path = args.get('file_path')

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

s3_client = boto3.client("s3")

username = "admin"
password = "Admin123"
url = "jdbc:redshift://test-for-poc.ck6iwmnrvsvz.ap-south-1.redshift.amazonaws.com:5439/dev"

def clean_file(s3_client, bucket, folder, file_path):
    spark = SparkSession.builder \
        .appName("Clean File and Write to S3") \
        .getOrCreate()

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
        logger.info("Getting list of objects in S3")
        
        obj = response.get("Contents")
        logger.info("Appending the S3 file paths for transformations")
        file_paths=[]
            
        l=[]
        for i in obj:
            if i['Key'].endswith('.csv'):
                l.append(i['Key'])
        print(l)
        
        logger.info("This functions transforms the data..")
        
        for i in l:
            s3_path = "s3://"+bucket+"/"+i
            print(s3_path)
            df = spark.read.option("header","True").csv(s3_path)
            
            logger.info("Converting file names to lowercase..")
            for col in df.columns:
                df=df.withColumnRenamed(col,col.lower())
                
            logger.info("Converting data type for some columns..")
            df=df.withColumn("salary",df.salary.cast('int'))
            
            logger.info("Writing dataframe to S3..")
            csv_buffer=io.StringIO()
            filename = file_path+'trnsf_emp'+'.csv'
            df.write.mode("overwrite").option("header", "true").csv(filename)
            
            # Creating dynamic dataframe
            dynamic_df = DynamicFrame.fromDF(df, glueContext, "test_nest")  
            connection_options_val = {"dbtable":"employee1","database":"dev"}
            
            # Writing to Redshift 
            glueContext.write_dynamic_frame.from_jdbc_conf(frame=dynamic_df, catalog_connection="glue-redshift", connection_options=connection_options_val, redshift_tmp_dir="s3://dev-s3-glue/temp_dir_redshift/")
            
            logging.info("Job completed successfully!")

    except Exception as e:
        logger.info(e)
        
clean_file(s3_client, bucket, folder, file_path)
