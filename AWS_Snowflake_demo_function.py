import snowflake.connector as sf

def run_query(conn, query):
    
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()

def lambda_handler(event, context):

    print('Main Execution Block Starts')
    print(event)
    user="dheeraj2112"
    password="@Dheeraj123"
    account="LKJREUJ-XX89936"
    database="EDW"
    warehouse="COMPUTE_WH"
    schema="EDW"
    role="ACCOUNTADMIN"


    conn=sf.connect(user=user,password=password,account=account);
    print(conn);

    statement_1='USE WAREHOUSE '+warehouse;
    statement3="USE DATABASE "+database;
    statement3_1="USE SCHEMA "+schema;
    statement4="USE ROLE "+role;
    print('------Executing Context Queries-------');
    print('');
    run_query(conn,statement_1);
    print(statement_1);
    run_query(conn,statement3);
    print(statement3);
    run_query(conn,statement3_1);
    print(statement3_1);
    run_query(conn,statement4);
    print(statement4);
    print('------Executing Main Queries-------');
    sql_query_table_creation = "CREATE OR REPLACE TABLE test_table(col1 integer, col2 string)";
    print(sql_query_table_creation);
    run_query(conn, sql_query_table_creation);
    sql_query_data_ingestion="INSERT INTO test_table(col1, col2) VALUES    (123, 'test string1'), (456, 'test string2')";
    run_query(conn, sql_query_data_ingestion);
    print(sql_query_data_ingestion);
    print('');
    print('------Execution Successful--------');
    print('Main Execution Block Ends')