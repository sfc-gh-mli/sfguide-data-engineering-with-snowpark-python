from snowflake.snowpark import Session

import os, sys
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils import snowpark_utils
session = snowpark_utils.get_snowpark_session()

## replace 'TEST' with your FIRST NAME
MY_ID =  'TEST'
MY_WAREHOUSE = MY_ID +'_WH'
MY_DB = 'DB_'+ MY_ID

#create warehouse and use it
session.sql("create or replace warehouse "+MY_WAREHOUSE+" warehouse_type = standard warehouse_size = 'LARGE' auto_suspend = 600").collect()
session.use_warehouse(MY_WAREHOUSE)

#create database and use it
session.sql("create or replace database "+MY_DB+ " clone hol_db" ).collect()
session.use_database(MY_DB)

print(session.get_current_database())
print(session.get_current_warehouse())
print("------------------------------")
print("Session created.")

