#File 3: Stored Procedure Execution in Redshift

"""Summary:
Purpose: This script connects to a Redshift database and executes a stored procedure for data transformation and loading.
Key Functions:
Database Connection: Establishes a connection to Redshift using parameters provided via command line.
Stored Procedure Execution: Calls the scd2_emp stored procedure, which handles data transformation logic (like managing Slowly Changing Dimensions).
Commit Changes: Ensures that all changes made by the stored procedure are committed to the database."""

import psycopg2
from awsglue.utils import getResolvedOptions
import logging
import sys

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ['database','port','user','password','host'])

database = args.get('database')
port = args.get('port')
user = args.get('user')
password = args.get('password')
host = args.get('host')

conn_string = f"dbname={database} port={port} user={user} password={password} host={host}"
conn = psycopg2.connect(conn_string)

logger.info("Connection established")

cursor = conn.cursor()
cursor.execute("call scd2_emp();")

conn.commit()
cursor.close()
