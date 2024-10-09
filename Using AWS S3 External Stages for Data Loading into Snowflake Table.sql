--#########################################----------------------------------------------------------------------------
--######################################### Using AWS S3 External Stages for Data Loading into Snowflake Table
--#########################################----------------------------------------------------------------------------
--1. Use the appropriate role 

USE ROLE ACCOUNTADMIN;

--2. Use the Warehouse 

USE WAREHOUSE COMPUTE_WH;

--3. Create the database 

CREATE DATABASE DJ_WORLD;

--4. Create the schema 

CREATE SCHEMA DJ_WORLD.DJ;

--5. Follow the link for creating policy and roles needed for this.

https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration.html#step-1-configure-access-permissions-for-the-s3-bucket

--   create the policy and attcah that policy to the role used for this example.

--6 Create the storage integration and provide the role information accordingly.

CREATE OR REPLACE STORAGE INTEGRATION DJWROLD_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::322918619727:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('*')

--7 Describe the Storage Integration that is created

--STORAGE_AWS_IAM_USER_ARN
--STORAGE_AWS_EXTERNAL_ID
--Both to be put on trust settings of the role created.

DESC INTEGRATION DJWROLD_S3_INT

--8 Show and Describe the Stage

SHOW STAGES;

DESC STAGE DJ_S3_STAGE

--9 List the Stage

LIST  @DJ_WORLD.DJ.DJ_S3_STAGE

--10 Selecting the file contents (1 file as of now)

SELECT $1,$2,$3,$4,$5,TO_TIMESTAMP(TO_DATE($6,'DD-MON-YY')) as computed ,$7,$8,$9, $10  FROM @DJ_WORLD.DJ.DJ_S3_STAGE

--11 Provide the required priviliges to the roles 

GRANT CREATE STAGE ON SCHEMA DJ_WORLD.DJ TO ROLE ACCOUNTADMIN;

GRANT USAGE ON INTEGRATION DJWROLD_S3_INT TO ROLE ACCOUNTADMIN;

--12 Use Schema in the context 

USE SCHEMA DJ_WORLD.DJ;

--13 Create the required File Format

CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT 
TYPE = CSV
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1 
NULL_IF = ('NULL', 'NULL') 
EMPTY_FIELD_AS_NULL = TRUE ;

--Compression can be optional and based on need
compression = gzip;

--14 Create the External Stage using the required parameters

CREATE STAGE DJ_S3_STAGE
  STORAGE_INTEGRATION = DJWROLD_S3_INT
  URL = 's3://dheerajworld/srcfiles/'
  FILE_FORMAT = my_csv_format;

--15 Create the Table as per file structure 

CREATE OR REPLACE TABLE employees1
   ( employee_id VARCHAR
   , first_name VARCHAR
   , last_name VARCHAR
   , email VARCHAR
   , phone_number VARCHAR
   , hire_date VARCHAR
   , job_id VARCHAR
   , salary VARCHAR
   , commission_pct VARCHAR
   , manager_id VARCHAR
   , department_id VARCHAR
   ) ;

SELECT * FROM employees1;
TRUNCATE TABLE employees1;

--16 Run the copy into command to copy file data into table.

COPY 
 INTO employees1
  FROM @DJ_WORLD.DJ.DJ_S3_STAGE
  FILES = ('employees.csv')  -- FILES option
  FORCE=TRUE ; --if needed to forcelly loaded again

  COPY INTO employees1
  FROM @DJ_S3_STAGE
  PATTERN='.*employees.*.csv' --PATTREN OPTION
  FORCE=TRUE;

--17 Checking the Load History
--Truncate table will re-insert the data again and re-running the copy into will skip the load if the flle is alredy loaded.

SELECT * FROM DJ_WORLD.INFORMATION_SCHEMA.LOAD_HISTORY