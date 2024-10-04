# AWS Lambda Snowflake Project using Python3.8 connector.

This project is an integration from AWS Lambda to Snowflake Data Cloud for Data Analysis and Manipulation.


Snowflake is a powerful data warehouse platform that allows you to store, analyze, and query large amounts of structured and semi-structured data. AWS Lambda, on the other hand, is a serverless compute service that lets you run your code without provisioning or managing servers. In this sample project, we will explore how to use Snowflake with Python in AWS Lambda.

**Overview**

To interact with Snowflake from a Lambda function, we need to manage the integration using Python code. AWS Lambda does not provide built-in support for Snowflake, so we will configure Snowflake resources such as a Pipe and a Warehouse, along with AWS resources like aws.lambda.Function. We will write a Python-based Lambda function that connects to Snowflake and runs the necessary queries using Snowflake Python connector libraries.

**Setting up the Environment**

Before we dive into the code, we have to ensure we have the necessary dependencies and access privileges as required.

**Dependencies**

To communicate with Snowflake from a Lambda function, we need to install the snowflake-connector-python library. We can include this library in our Lambda function package. Additionally, we have to install the necessary dependencies for the Python code that will interact with Snowflake, such as the pandas and numpy libraries for data manipulation and analysis.

**The required Python 3.8 ZIP package is available with the necesaary dependencies and can be used in AWS Lambda Layers.**

https://github.com/dheeraj2112/AWS_Snowflake_Project/blob/9517b75fcb2b86216b14811ba173450fc628e5d2/snowflake_lambda_layer.zip 

**Access Privileges**

To connect to Snowflake from your Lambda function, you will need to provide appropriate credentials. Snowflake provides different authentication methods, such as username/password, single sign-on (SSO), and multi-factor authentication (MFA). You need to ensure that you have the required credentials and access privileges to connect to Snowflake.

**Writing the Lambda Function**

Now that we have the necessary environment set up, we can proceed to write our Lambda function code. In this example code, we will assume that you have a Python function that interacts with Snowflake to create/retrieve data.

A sample demo code is available for use at the repo. https://github.com/dheeraj2112/AWS_Snowflake_Project/blob/9517b75fcb2b86216b14811ba173450fc628e5d2/AWS_Snowflake_demo_function.py
