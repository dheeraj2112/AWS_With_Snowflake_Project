--#################################################################################################
-- AWS to Snowflake--> Data Loading PREP steps 
--#################################################################################################

--1. Use the appropriate role 

USE ROLE ACCOUNTADMIN;

--2. Use the Warehouse 

USE WAREHOUSE COMPUTE_WH;

--3. Use the database, EDW

USE EDW;

--4. Use the schema, EDW_STG

USE EDW.EDW_STG;

--5. Follow the link for creating policy and roles needed for this.

https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration.html#step-1-configure-access-permissions-for-the-s3-bucket

-- Create the policy and attcah that policy to the role used for this example.

--6 Create the storage integration and provide the AWS role information accordingly.

CREATE OR REPLACE STORAGE INTEGRATION DJWORLD_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::322918619727:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('*')

--Integration DJWORLD_S3_INT successfully created.


--7 Describe the Storage Integration that is created and copy the values for the below paramters.
--Both to be put on trust settings of the AWS role created.
-->    STORAGE_AWS_IAM_USER_ARN:: arn:aws:iam::711387127720:user/9lbr0000-s
-->    STORAGE_AWS_EXTERNAL_ID:: YU63937_SFCRole=2_mvlsvEvk2QeWUmlE5njevNh7Fq0=


DESC INTEGRATION DJWORLD_S3_INT

/*
--Update the trust policy for the IAM role created in AWS.

{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::711387127720:user/9lbr0000-s"
			},
			"Action": "sts:AssumeRole",
			"Condition": {
				"StringEquals": {
					"sts:ExternalId": "YU63937_SFCRole=2_mvlsvEvk2QeWUmlE5njevNh7Fq0="
				}
			}
		}
	]
}

*/

--8 Creating the file format as required. 

CREATE OR REPLACE FILE FORMAT AWS_CSV_FORMAT 
TYPE = CSV
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1 
NULL_IF = ('NULL', 'NULL') 
EMPTY_FIELD_AS_NULL = TRUE 
FIELD_OPTIONALLY_ENCLOSED_BY = '\042';

--Compression can be optional and based on need.
compression = gzip;

--9 Create the External Stage using the required parameters like Storage Integration, URL and File Format.

CREATE OR REPLACE STAGE AWS_S3_STAGE
  STORAGE_INTEGRATION = DJWORLD_S3_INT
  URL = 's3://dheerajworld/srcfiles/'
  FILE_FORMAT = AWS_CSV_FORMAT;
  

--10 Show and Describe the Stage

SHOW STAGES LIKE '%AWS_S3_STAGE%';

--11 Describe the Stage.

DESC STAGE AWS_S3_STAGE;

--12 List the Stage to see the files informations 

LIST  @AWS_S3_STAGE

--The contents of the S3 buckets will be refected here as per bucket path specified earlier.

name	                                        size	               md5	                      last_modified
s3://dheerajworld/srcfiles/HR_COUNTRIES.csv		408		71355dd812b077afe2e5f9d569ee9a6b	Sun, 20 Oct 2024 15:11:35 GMT
s3://dheerajworld/srcfiles/HR_DEPARTMENTS.csv	689		1d7c9808dd16ef0da1ce8f621b0bf27e	Sun, 20 Oct 2024 15:11:36 GMT
s3://dheerajworld/srcfiles/HR_EMPLOYEES.csv		9525	9cab4e5f435d72a501263c0da36b57b5	Sun, 20 Oct 2024 15:11:37 GMT
s3://dheerajworld/srcfiles/HR_JOBS.csv			769		da5c590a89118f004bc2becf2dc70415	Sun, 20 Oct 2024 15:11:38 GMT
s3://dheerajworld/srcfiles/HR_JOB_HISTORY.csv	700		00e398a8c62a1b5e7afd2b274d59d652	Sun, 20 Oct 2024 15:11:37 GMT
s3://dheerajworld/srcfiles/HR_LOCATIONS.csv		1279	128932b818052bf97cefc4e27ac499d2	Sun, 20 Oct 2024 15:11:38 GMT
s3://dheerajworld/srcfiles/HR_REGIONS.csv		79		4b1a470171be32a122061c8dd7f26f7b	Sun, 20 Oct 2024 15:11:39 GMT


--13 Provide the required priviliges to the roles, if you face access issues for the role that you are using.

GRANT CREATE STAGE ON SCHEMA DJ_WORLD.DJ TO ROLE ACCOUNTADMIN;

GRANT USAGE ON INTEGRATION DJWROLD_S3_INT TO ROLE ACCOUNTADMIN;

--14

--Reference COPY Command(s) to Load/Validate Data (To be used directly in Tasks or Pipes which are mentioned in next steps for loading process)
--into Staging tables by converting appropriate data types during the data load process itself wherever required as per DDLs for the EDW_STG and EDW Layers.

--Note that a single s3 bucket folder has been used to load the files hence the pattern parameter is added in the copy command to select appropriate file needed for the STG load.

--REGIONS LOAD
 
COPY INTO EDW.EDW_STG.REGIONS 
    FROM
        (SELECT $1::NUMBER,$2::VARCHAR2(25),MD5($2::VARCHAR2(25)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @AWS_S3_STAGE)
		 PATTERN='.*REGIONS.csv.*' ;
		 
--COUNTRIES LOAD 
		 
COPY INTO EDW.EDW_STG.COUNTRIES 
    FROM
        (SELECT $1::CHAR(2),$2::VARCHAR2(40),$3::NUMBER,MD5($2||$3::VARCHAR2),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @AWS_S3_STAGE)
		 PATTERN='.*COUNTRIES.csv.*' ;

--LOCATIONS LOAD 

COPY INTO EDW.EDW_STG.LOCATIONS
    FROM
        (SELECT $1::NUMBER(4),$2::VARCHAR2(40),$3::VARCHAR2(12),$4::VARCHAR2(30),$5::VARCHAR2(25),$6::CHAR(2),MD5($2||$3||$4||$5||$6),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @AWS_S3_STAGE)
		 PATTERN='.*LOCATIONS.csv.*' ;


--DEPARTMENTS LOAD

COPY INTO EDW.EDW_STG.DEPARTMENTS 
    FROM
        (SELECT $1::NUMBER(4),$2::VARCHAR2(30),$3::NUMBER(6),$4::NUMBER(4),MD5($2||$3::VARCHAR2(6)||$4::VARCHAR2(4)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @AWS_S3_STAGE)
		 PATTERN='.*DEPARTMENTS.csv.*' ;
		 
--JOBS LOAD		

COPY INTO EDW.EDW_STG.JOBS
    FROM
        (SELECT $1::VARCHAR2(10),$2::VARCHAR2(35),$3::NUMBER(6),$4::NUMBER(6),MD5($2||$3::VARCHAR2(6)||$4::VARCHAR2(6)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @AWS_S3_STAGE)
		 PATTERN='.*JOBS.csv.*' ;
		 
--EMPLOYEES LOAD 

COPY INTO  EDW.EDW_STG.EMPLOYEES
    FROM
        (SELECT $1::NUMBER(6),$2::VARCHAR2(20),$3::VARCHAR2(25),$4::VARCHAR2(25),$5::VARCHAR2(20),$6::DATE,$7::VARCHAR2(10),$8::NUMBER(8,2),$9::NUMBER(2,2),$10::NUMBER(6),$11::NUMBER(4)
		,MD5($2||$3||$4||$5||DATE($6)::VARCHAR2(10)||$7||$8::VARCHAR2(10)||$9::VARCHAR2(4)||$10::VARCHAR2(6)||$11::VARCHAR2(4)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @AWS_S3_STAGE)
		 PATTERN='.*EMPLOYEES.csv.*' ;
		 
--JOB_HISTORY LOAD 

COPY INTO EDW.EDW_STG.JOB_HISTORY
    FROM
        (SELECT $1::NUMBER(6),$2::DATE,$3::DATE,$4::VARCHAR2(10),$5::NUMBER(4),MD5(DATE($2)::VARCHAR2(10)||DATE($3)::VARCHAR2(10)||$4||$5::VARCHAR2(4)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @AWS_S3_STAGE)
		 PATTERN='.*JOB_HISTORY.csv.*' ;

--17 Checking the Load History
--Truncate table will re-insert the data again and re-running the copy into will skip the load if the flle is alredy loaded.

SELECT * FROM EDW.INFORMATION_SCHEMA.LOAD_HISTORY;