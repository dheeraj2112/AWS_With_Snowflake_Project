**AWS and Snowflake End-to-End Data Warehousing Project Overview and Summary**

1. Project Overview
The goal of this project is to implement a data warehousing solution using AWS and Snowflake, enabling efficient data storage, processing, and analysis. This end-to-end project covers data ingestion, transformation, storage, and reporting, aiming to provide a unified view of business data for better decision-making.

1.1) Pre-requisite:

i. Valid Snowflake and AWS Accounts

ii. External Snowflake Stage with Storage Integration pointing to AWS S3 Bucket with appropriate IAM role and policies.

iii. On-Prem Oracle db HR schema objects exported in AWS S3 Bucket in CSV format which are the source files for this project. The object exports can obtained from step #2 mentioned below, if not available already.

1.2) Reference Architecture Diagram

![image](https://github.com/user-attachments/assets/325e1110-6cae-4eb9-8ae1-cdb651806d52)


2.  Source and Target
   
Source: Source is Oracle on-prem HR schema objects exports in AWS S3 in CSV format. The source files can be used from below link, if not available handy.

https://github.com/dheeraj2112/AWS_With_Snowflake_Project/tree/master/AWS%20Snowflake%20End%20to%20End%20Project/2.Source%20Files

Target: Target is Snowflake having both EDW_STG and EDW Layers. The DDLs can be found in the repo. 

https://github.com/dheeraj2112/AWS_With_Snowflake_Project/blob/master/AWS%20Snowflake%20End%20to%20End%20Project/3.%20Snowflake%20EDW_STG_EDW_Layers_DDLs/ORA_HR_TO_SNFLK_EDW_LAYERS_DDL_ALL.sql

3.  ETL/ELT Data Flows using Snowflake Tasks or Snow PIPEs.

**i.** Source (CSV Files) to EDW_STG (Snowflake) : The code repo can be found here to Create the External STAGE(s), Storage Integration and FILE FORMATS then run the COPY INTO  command from SQL Worksheet, Tasks or PIPE(s) to load these CSV files into the respective STG table(s).

https://github.com/dheeraj2112/AWS_With_Snowflake_Project/blob/master/AWS%20Snowflake%20End%20to%20End%20Project/4.%20AWS%20Snowflake%20Scripts/0.%20AWS%20to%20Snowflake%20Data%20Loading%20PREP%20steps.sql

**The code repo for loading the EDW_STG tables can be found here using Snowflake Tasks.**

https://github.com/dheeraj2112/AWS_With_Snowflake_Project/blob/master/AWS%20Snowflake%20End%20to%20End%20Project/4.%20AWS%20Snowflake%20Scripts/1.%20AWS%20S3%20SRC%20to%20EDW_STG%20Pipelines%20using%20Snowflake%20Tasks.sql

Optional: SRC to EDW_STG data pipelines can be implemented using Snowflake feature such Snowflake SnowPipe as well.We can Setup Snowpipe to load data from files in a stage into staging tables. Once the PIPE(s) are created then use the refresh command to load STG tables manually.

**Note 1:** The REFRESH functionality is intended for short term use to resolve specific issues when Snowpipe fails to load a subset of files and is not intended for regular use.
The REFRESH functionality in the Snowpipe is explicitly to load existing or scan for new files. In a production environment, you'll likely enable AUTO_INGEST, connecting it with your cloud storage events (like AWS SNS) and process new files automatically.

**Note 2:** To reload the data from External Stage, you must either specify FORCE = TRUE in COPY INTO command or modify the file and stage it again, which generates a new checksum.By default, even truncating the STG tables will not re-load data into these tables from External Stage unlike Internal Stages.

The code repo can be found here.

https://github.com/dheeraj2112/AWS_With_Snowflake_Project/blob/master/AWS%20Snowflake%20End%20to%20End%20Project/4.%20AWS%20Snowflake%20Scripts/1.1%20AWS%20S3%20SRC%20to%20EDW_STG%20Pipelines%20using%20Snowflake%20Pipe.sql

**ii.** EDW_STG (Snowflake) to EDW(Snowflake) : Using SCD Type-2 (Using MERGE SQL statements), the code repo can be found here.

https://github.com/dheeraj2112/AWS_With_Snowflake_Project/blob/master/AWS%20Snowflake%20End%20to%20End%20Project/4.%20AWS%20Snowflake%20Scripts/2.%20Snowflake%20EDW_STG%20to%20EDW%20Pipelines%20using%20Snowflake%20Tasks.sql

**iii.** Orchestration and scheduling : Using Snowflake Tasks as per needed schedule with SCHEDULE parameter.
   
4.  Load Validations

The code repo can be found here to validate the SRC to EDW_STG tables load as well as EDW_STG to EDW tables load.

https://github.com/dheeraj2112/AWS_With_Snowflake_Project/blob/master/AWS%20Snowflake%20End%20to%20End%20Project/4.%20AWS%20Snowflake%20Scripts/3.%20EDW_STG%20and%20EDW%20Pipelines%20Load%20Validations.sql
   
5.  Analysis and Reporting (ETL Code)

Views or Dynamic tables can be leveraged accordingly for the analytics and reporting or visualization needs. This can be done from Snowflake specific features as per need.

https://github.com/dheeraj2112/AWS_With_Snowflake_Project/blob/master/AWS%20Snowflake%20End%20to%20End%20Project/4.%20AWS%20Snowflake%20Scripts/4.%20EDW_EXTR%20Views%20and%20Dynamic%20Tables%20for%20Reporting.sql
   
6.  Next Steps **-->**
    
i. Incremental/CDC logic handling within Snowflake Data Cloud by leveraging the Streams coupled with TASKs to process them as needed.

ii. Addtional integration with Azure/GCP in place of AWS S3 Bucket to implement this project. Just SRC to EDW_STG data pipelines logic to be implemented as EDW_STG to EDW data pipelines are already in place within Snowflake as detailed out above.

**ENDOFDOCUMENT**

