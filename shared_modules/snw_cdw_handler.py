import re
import os
import pathlib
import logging
import tempfile
import traceback
import snowflake.connector as sf
from datetime import datetime as dt
from snowflake.connector import Error
from pandas.core.frame import DataFrame
from snowflake.connector.errors import ProgrammingError
from snowflake.connector.connection import SnowflakeConnection


## Logging
# create logger with '__main__'
logger = logging.getLogger('__main__.' + __name__)

TEMPFILE_PATH = tempfile.gettempdir()

#################################################################
################# SNOWFLAKE DATA CDW UTILITIES ##################
#################################################################

def connect_to_db(user:str,password:str,account:str,session_parameter={"QUERY_TAG": "Python Wrapper"}) -> SnowflakeConnection:
    """
    Connect to an Snowflake DB 

    Parameters
    user: username associate to the snowflake account
    password: associated password for the snowflake account
    account: associated snowflake account placement
    warehose: assocated compute cluster
    session_parameter: Idetifier for the session
    """
    snow_conn = None

    try:
        snow_conn = sf.connect(
                        user=user,
                        password=password,
                        account=account,
                        session_parameter=session_parameter,
                        )
        logger.info("Connection to the Cloud Data Warehouse successfully initiatied")
        return snow_conn

    except Error as e:
        logger.error(f'Following exception {e} as occured', exc_info=True)
    return snow_conn


def est_connection(conn:str, database:str,compute_wh:str,schema:str, user_role:str) -> None:
    '''
    Establish connection to the appropriate database, schema, warehouse

    Parameters
    conn - Connectiontion object
    database: Database in Snowflake that requires the connections
    compute_wh: The compute warehouse the needs to be used
    schema: The schema in the DB that needs to be used
    user_role: The user role that has to be assumed 
    '''
    if conn is not None:

        # Set user role
        try:
            sql = 'USE ROLE {}'.format(user_role)
            execute_query(conn, sql)
            logger.info(f"Using role {user_role}")
        except ProgrammingError as e:
            logger.error(f"The following error has was raised {e}", exc_info=True)

        # Set/Create database to be used
        try:
            sql = 'USE DATABASE {}'.format(database)
            execute_query(conn, sql)
            logger.info(f"Using database {database}")
        except ProgrammingError as e:
            try:
                if e.errno == 2045:
                    logger.info(f"Database {database} does not exist, creating it...")
                    sql = f'CREATE DATABASE IF NOT EXISTS {database}'
                    execute_query(conn, sql)
            except Error as e:
                logger.error(f"The following error has was raised {e}", exc_info=True)

        # Set/Create Compute Warehouse
        try:
            sql = 'USE WAREHOUSE {}'.format(compute_wh)
            execute_query(conn, sql)
            logger.info(f"Using warehouse {compute_wh}")
        except ProgrammingError as e:
            try:
                if e.errno == 2045:
                    logger.warn(f"Warehouse {compute_wh} does not exist, creating it...")
                    sql = f'CREATE WAREHOUSE IF NOT EXISTS {compute_wh}'
                    execute_query(conn, sql)
            except Error as e:
                logger.error(f"The following error has was raised {e}", exc_info=True)

        # Set/Create Schema
        try:
            sql = 'USE SCHEMA {}'.format(schema)
            execute_query(conn, sql)
            logger.info(f"Using schema {schema}")
        except ProgrammingError as e:
            try: 
                if e.errno == 2045:
                    logger.warn(f"Schema {schema} does not exist, creating it...")
                    sql = f'CREATE SCHEMA IF NOT EXISTS {schema}'
                    execute_query(conn, sql)
            except Error as e:
                logger.error(f"The following error has was raised {e}", exc_info=True)

        # Resume warehouse if suspended
        try:
            sql = 'ALTER WAREHOUSE {} RESUME'.format(compute_wh)
            execute_query(conn, sql)
            logger.info("CDW related environment variables successfuly set")
            return None
        except Error as e:
            logger.info("CDW related environment variables successfuly set")
            logger.warning(f"This exeption occured during resuming the warehouse {e}")
            return None

    else:
        logger.error("Connection variable is None")
        raise TypeError


def execute_query(connection:SnowflakeConnection, query:str) -> None:
    '''
    Create a standalone cursor for executing a sql statement
    '''
    if connection is not None:
        cursor = connection.cursor()
        cursor.execute(query)
        logger.debug(f"SQL: '{query}' query executed successfully {cursor.fetchone()}")
        cursor.close()
        return None
    else:
        logger.error("Connection variable is None")
        raise TypeError


def create_table(table_name:str, columns:dict) -> str:
    '''
    Create table in Snowflake

    Parameters:
    table_name: Name of the table to be creatad
    columns: Columns to be considered for creation of the table
    _type = Type of table to be created 
    '''
    # Creating column string from the dictionary
    pattern = re.compile(r'[ /.]+', re.IGNORECASE)
    col_str = "(" + ", ".join([re.sub(pattern,'_',k)+" "+v for k,v in columns.items()]) + ")"
    # print("Columns String fro create_table", col_str)
    try:
        sql = f'CREATE TABLE IF NOT EXISTS {table_name} {col_str};'
        logger.info(f"SQL string for create table for table name {table_name} generated successfully")
        logger.debug(f"Query: {sql}")
    except Exception as e:
        logger.error(f'Following exception {e} as occured', exc_info=True)
    return sql


def drop_table(conn:SnowflakeConnection, table_name:str) -> list:
    '''
    Function to drop the table 

    Parameters:

    conn: connection object
    table_name: Name of the table on which the drop actions needs to be carried out
    '''
    if conn is not None:
        c = conn.cursor()
        sql =   f'DROP TABLE IF EXISTS {table_name}'
        try:
            c.execute(sql)
            status = c.fetchall()
            c.close()
            logger.info(f"Drop operation on the table name {table_name} carried out successfully")
            return status
        except Error as e:
            logger.error(f'Following exception {e} as occured', exc_info=True)
            return None
    else:
        logger.error("Connection variable is None")
        raise TypeError


def insert_into(conn:SnowflakeConnection, to_table_name:str, from_table_name:str,database:str) -> None:
    '''
    Function to Insert contents from one table into another table

    Parameters:
    conn: Connection object
    to_table_name: Name of the table to be the insert action needs to be carried out
    from_table_name: Name of the table from which the insert action needs to be carried out
    
    '''
    if conn is not None:
        c = conn.cursor()
        
        # Insert into using permenant table
        cols_to = show_columns(conn=conn, database=database, table_name=to_table_name)
        cols_from = show_columns(conn=conn, database=database, table_name=from_table_name)
        
        # Sorting columns
        cols_to.sort()
        cols_from.sort()
        
        cols_to_str = "("+",".join(cols_to)+")"
        cols_from_str = ",".join(cols_from)
        
        sql = f'''
                INSERT INTO {to_table_name}{cols_to_str} 
                    SELECT 
                        {cols_from_str} 
                    FROM {from_table_name};
               '''
        try:
            c.execute(sql)
            status = c.fetchall()
            c.close()
            logger.info(f"Insert into table - {to_table_name} from table - {from_table_name} completed successfully ")
            return status
        except Error as e:
            logger.error(f'Following exception {e} as occured', exc_info=True)
            return None
    else:
        logger.error("Connection variable is None")
        raise TypeError



# Load csv file into temporary table
def copy_into_table(conn:SnowflakeConnection,df:DataFrame,csv_output_name:str,csv_file_path:str, data_stage:str,table_name:str,_type='TEMPORARY') -> tuple:
    '''
    Functions to copy the contents of a table in csv format into a temporary table

    Parameters:
    conn: Connection object
    df: dataframe object of the table that needs to be staged and copied
    csv_output_name: name of the csv file that will be stored in persistent storage for PUT action into stage
    data_stage: name of the data stage that needs to be used 
    table_name: name of the table that should be used for creating the temporary table

    Note: extension "temp_" is automatically prefixed into the table_name defined
    '''

    # Define path for storage of local logs as CSV and create directory if not exists
    CSV_FILE_PATH = os.path.join(TEMPFILE_PATH, csv_file_path)
    pathlib.Path(CSV_FILE_PATH).mkdir(parents=True, exist_ok=True)

    # Prefix the "temp_" keyword to the table name if _type is specified so
    if _type.upper() == 'TEMPORARY':
        table_name = f'temp_{table_name}'
        
    
    # Copy into action to begin
    if conn is not None:
        try:
            columns = list(df.columns)
            columns = [col.replace('.','__').upper() for col in columns]
            columns_add = {f"{extra}":"TEXT" for extra in columns}  
            logger.debug(f"Columns to add {columns_add}")
            
            # Preprocessing he Dataframe and saving it as a csv file
            try:
                df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
            except ValueError:
                df.replace("\n", "", inplace=True)
            except:
                logger.error("uncaught expception: %s", traceback.format_exc())

            # Staging the dataframe locally
            df.to_csv(os.path.join(CSV_FILE_PATH,f"{csv_output_name}.csv"),sep="\t", index=False, line_terminator="\n", encoding='utf-8')

            # Pulling the cursor
            c= conn.cursor()

            # Drop data stage incase it exists
            sql = f'DROP STAGE IF EXISTS {data_stage}'
            c.execute(sql)

            # Create data stage
            sql = f'CREATE STAGE {data_stage} file_format = (type = "csv" field_delimiter = "\t" skip_header = 1)'
            c.execute(sql)

            # Relative reference of the csv file
            csv_file = os.path.join(CSV_FILE_PATH,f"{csv_output_name}.csv")
            sql = "PUT file://" + csv_file + f" @{data_stage} auto_compress=true"
            c.execute(sql)

            # Drop temporary table if exists    
            sql = f'DROP TABLE IF EXISTS {table_name}'
            c.execute(sql)

            # Create temporary table
            sql = create_table(f"{table_name}", columns_add)
            c.execute(sql)

            # Copy into  table
            sql = f'COPY INTO {table_name} FROM @{data_stage}/{csv_output_name}.csv.gz file_format = (type = "csv" field_delimiter = "\t" skip_header = 1)' \
                'ON_ERROR = "ABORT_STATEMENT" '
            c.execute(sql)
            
            status = c.fetchall()
            c.close()
            logger.debug('Function Copy into table exectuted successfully')
            return status,columns, f"@{data_stage}/{csv_output_name}.csv.gz", table_name 
        except Error as e:
            logger.exception(f'Following exception {e} as occured {traceback.format_exc()}')
            return None
    else:
        logger.error("Connection variable is None")
        raise TypeError


# return column names from table
def show_columns(conn:sf.connect, database:str, table_name:str) -> list:
    '''
    Function to display the columns of a particular table

    Parameters:
    conn: Connection object
    database: Name of the database that houses the table
    table_name: Table name
    '''
    if conn is not None:
        c = conn.cursor()
        sql =   f'''
                SELECT DISTINCT COLUMN_NAME 
                    FROM "{database}"."INFORMATION_SCHEMA"."COLUMNS" 
                    WHERE TABLE_NAME ILIKE \'{table_name}\'
                '''
        try:
            c.execute(sql)
            status = c.fetchall()
            status = list(map(lambda x: x[0], status))
            c.close()
            logger.debug(f"Show columns operation on the table name {table_name} carried out successfully")
            return status
        except Error as e:
            logger.error(f'Following exception {e} as occured', exc_info=True)
            return None
    else:
        logger.error("Connection variable is None")
        raise TypeError


def get_tables_in_db(conn:SnowflakeConnection, database:str, table_name:str) -> list:
    '''
    Function to get tables from the db

    Parameters:
    conn: Connection Object
    database: The database for which the tables need to be listed
    table_name: name of tables which needs to be compared
    '''
    if conn is not None:
        try:
            c = conn.cursor()
            sql = f'''select distinct TABLE_NAME from "{database}"."INFORMATION_SCHEMA"."TABLES" where TABLE_NAME ILIKE '{table_name}%';'''
            c.execute(sql)
            result = c.fetchall()
            logger.debug(f"Show columns operation on the table name {table_name} carried out successfully")
            c.close()
            return result
        except Error as e:
            logger.error(f'Following exception {e} as occured {traceback.format_exc()}')
            return None
    else:
        logger.error("Connection variable is None")
        raise TypeError


# Alter table to add more columns
def alter_add_columns(conn:sf.connect, table_name:str, extra_columns:dict) -> list:
    '''
    Function to alter table and add columns

    Parameters:
    conn: Connection Object
    table_name: name of the tables fir which the tables are to be added
    extra_columns: dict object of the extra columns that are to be added to the table
    '''
    col_str = "(" + ", ".join([k.replace(".","__")+" "+v for k,v in extra_columns.items()]) + ")"
    if conn is not None:
        c = conn.cursor()
        sql =   f'ALTER TABLE IF EXISTS {table_name} ADD {col_str}'
        try:
            c.execute(sql)
            status = c.fetchall()
            c.close()
            logger.debug(f"Add columns operation on the table name {table_name} carried out successfully")
            return status 
        except Error as e:
            logger.error(f'Following exception {e} as occured', exc_info=True)
            return None
    else:
        logger.error("Connection variable is None")
        raise TypeError


def insert_into_permenant(conn, df, csv_output_name, csv_file_path, data_stage, table_name, database):
    '''
    Function to insert data from temporary table to permenant table

    Parameters:
    df: Dataframe that needs to be considered
    csv_output_name: name of the file that under which the csv file is to be stored in persistent storage 
    data_stage: name of the data stage
    table_name: name of the table into which the data should be inserted to
    database: the database under which the permenant table should be stored under
    '''
    if conn is not None:
        try:
            status=[]

            # Step 1- Create temporary table anyway
            test = copy_into_table(conn=conn,df=df,csv_output_name=csv_output_name,csv_file_path=csv_file_path, data_stage=data_stage,table_name=table_name,_type='TEMPORARY')
            
            # Assigning the temporary columns variable
            columns_tmp = test[1]

            # Append status
            status.append(test[0])

            # Extract columns from table
            columns_snw = show_columns(conn=conn, database=database, table_name=table_name)

            if set(columns_tmp) == set(columns_snw):
                status.append(
                    insert_into(
                        conn=conn, 
                        to_table_name=table_name, 
                        from_table_name=f"temp_{table_name}",
                        database=database
                        )
                    )
                status.append(
                    drop_table(
                        conn=conn, 
                        table_name=f"temp_{table_name}"
                        )
                    )
                logger.debug("Columns consistent; insert as is..")
                return  status
            elif set(columns_tmp) - set(columns_snw) != set():
                # Identify delta columns
                extra_columns = set(columns_tmp) - set(columns_snw)
                extra_columns = {f"{extra}":"TEXT" for extra in extra_columns}
                # Add delta columns
                status.append(
                    alter_add_columns(
                        conn=conn, 
                        table_name=table_name, 
                        extra_columns=extra_columns
                        )
                    )
                # Carry out insert operation
                status.append(
                    insert_into(
                        conn=conn, 
                        to_table_name=table_name, 
                        from_table_name=f"temp_{table_name}", 
                        database=database
                        )
                    )
                status.append(
                    drop_table(
                        conn=conn, 
                        table_name=f"temp_{table_name}"
                        )
                    )
                logger.debug(f"Columns inconsistent: Temp table has more columns {set(columns_tmp) - set(columns_snw)}; Adjusting the permenant table for the insert")
                return status
            elif set(columns_snw) - set(columns_tmp) != set():
                # Identify delta columns
                extra_columns = set(columns_snw) - set(columns_tmp)
                extra_columns = {f"{extra}":"TEXT" for extra in extra_columns}
                # Add delta columns
                status.append(
                    alter_add_columns(
                        conn=conn, 
                        table_name=f"temp_{table_name}", 
                        extra_columns=extra_columns
                        )
                    )
                # Carry out insert operation
                status.append(
                    insert_into(
                        conn=conn, 
                        to_table_name=table_name, 
                        from_table_name=f"temp_{table_name}",
                        database=database)
                )
                status.append(
                    drop_table(
                        conn=conn, 
                        table_name=f"temp_{table_name}"
                        )
                ) 
                logger.debug(f"Columns inconsistent: Permenant table has more columns {set(columns_snw) - set(columns_tmp)}; Adjusting the temporary table for the insert")
                return status
        except Exception as e:
            logger.error(f'Following exception {e} as occured', exc_info=True)
            return None
    else:
        logger.error("Connection variable is None")
        raise TypeError



def orchestrate(conn:SnowflakeConnection,df:DataFrame,table_name:str,database:str,csv_filename:str, csv_file_path:str,data_stage:str) -> list:
    '''
    Function that takes in a datafram as an input to orchestrate the Copy Into or Insert Into based on the presence or absence of the table in the target database
    
    Parameters:
    conn: Snowflake Connection object
    df: dataframe 
    table_name: name of the table to be created or inserted into
    database: name of the database within which the table needs to be inserted
    csv_filename: name of the csv file underwhich the it should be stored in local stage
    data_stage: name of the snowflake internal data stage
    
    '''
    
    if conn is not None:
        test = get_tables_in_db(conn, database, table_name)
        table_n = table_name.upper()
        try:
            try:
                if table_n in test[0] : #If table exists already
                    status = insert_into_permenant(
                        conn=conn, 
                        df=df, 
                        data_stage=data_stage, 
                        table_name=table_name, 
                        database=database, 
                        csv_output_name=csv_filename, 
                        csv_file_path=csv_file_path
                        )
                    select_distinct(
                        conn=conn,
                        database=database,
                        table_name=table_name,
                        temp_table_name=f'TEMP_{table_name}_STAGE'
                    )
                    logger.info(f"Permenant insert into executed successfully into table {table_name}")
                    return status[0]
                else: #If there are tables with similar names
                    status = copy_into_table(
                        conn=conn, 
                        df=df, 
                        csv_output_name=csv_filename, 
                        csv_file_path=csv_file_path, 
                        data_stage=data_stage, 
                        table_name=table_name, 
                        _type="PERMENANT"
                        )
                    select_distinct(
                        conn=conn,
                        database=database,
                        table_name=table_name,
                        temp_table_name=f'TEMP_{table_name}_STAGE'
                    )
                    logger.info(f"Permenant copy into executed successfuly into table {table_name}")
                    return status[0]
            except IndexError:# If there are no tables
                status = copy_into_table(
                    conn=conn, 
                    df=df, 
                    csv_output_name=csv_filename, 
                    csv_file_path=csv_file_path, 
                    data_stage=data_stage, 
                    table_name=table_name, 
                    _type="PERMENANT"
                    )
                select_distinct(
                    conn=conn,
                    database=database,
                    table_name=table_name,
                    temp_table_name=f'TEMP_{table_name}_STAGE'
                )
                logger.info(f"Permenant copy into executed successfuly into table {table_name}")
                return status[0]
            except:
                logger.error("Uncaught expception: %s", traceback.format_exc())
                return None

        except Exception as e:
            logger.exception(f'Following exception {e} as occured {traceback.format_exc()}')
            return None

    else:
        logger.error("Connection variable is None")
        raise TypeError


def logs_to_stage(conn:SnowflakeConnection,df:DataFrame,data_stage:str,csv_output_name:str, csv_file_path:str, drop=True ) -> None:
    '''
    Bespoke funtion to move the logfiles into stage
    Note: Can be used for any operation to stage the dataframe as a CSV file with pipe separators

    Parameters:
    conn: Snowflake connection object
    df: Pandas Dataframe to be staged
    data_stage: name of the data_stage under which which the file needs to be stored
    csv_output_name: name of the csv file under which it should be stored in the local storage
    drop: if true, will drop the stahe and create fresh 
    '''
    if conn is not None:
        # Define path for storage of local logs as CSV and create directory if not exists
        CSV_FILE_PATH = os.path.join(TEMPFILE_PATH, csv_file_path)
        pathlib.Path(CSV_FILE_PATH).mkdir(parents=True, exist_ok=True)

        # Carry out the load to stage
        try:
            # Preprocessing he Dataframe and saving it as a csv file
            try:
                df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
            except ValueError:
                df.replace("\n", "", inplace=True)
            except :
                logger.error("uncaught expception: %s", traceback.format_exc())

            # Staging the dataframe locally
            df.to_csv(os.path.join(CSV_FILE_PATH,f"{csv_output_name}.csv"),sep="\t", index=False, line_terminator="\n", encoding='utf-8')

            # Pulling the cursor
            cur= conn.cursor()
            
            if drop:
                # Drop data stage incase it exists
                sql = f'DROP STAGE IF EXISTS {data_stage}'
                cur.execute(sql)

            # Create data stage
            sql = f'CREATE STAGE IF NOT EXISTS {data_stage}  file_format = (type = "csv" field_delimiter = "\t" skip_header = 1)'
            cur.execute(sql)

            # Relative reference of the csv file
            csv_file = os.path.join(CSV_FILE_PATH,f"{csv_output_name}.csv")
            sql = "PUT file://" + csv_file + f" @{data_stage} auto_compress=true"
            cur.execute(sql)

            cur.close()
            logger.info(f"Dataframe {df} has been staged in {data_stage} successfully")
            return None
        except:
            logger.error("Uncaught expception: %s", traceback.format_exc())
            return None
    else:
        logger.error("Connection variable is None")
        raise TypeError


def file_exists_stage(conn, data_stage, filename):
    '''
    Function to check if a file exists in a stage. Returns a version controlled function name based on existence or absence

    Parameters:
    conn: Snowflake connection parameter
    data_stage: Name of the internal data stage to be check in
    filename: Name of the file name to be checked for in the stage
    '''
    if conn is not None:
        ver_cnt = 0

        try:
            cur = conn.cursor()
            sql = f'LIST @{data_stage}'
            cur.execute(sql)
        except ProgrammingError:
            filename = filename + f'_V{ver_cnt}'
            return filename

        result = cur.fetchall()
        cur.close()
        if (len(result) > 0):
            wrk_list = [each[0].upper().split('/')[-1].split('.')[0] for each in result]

        test_filename = filename + f'_V{ver_cnt}'
        while test_filename.upper() in wrk_list:
            ver_cnt += 1
            test_filename = filename + f'_V{ver_cnt}'
        
        filename = filename + f'_V{ver_cnt}'

        return filename
    else:
        logger.error("Connection variable is None")
        raise TypeError


def context_update(conn:SnowflakeConnection, NAME:str, CONT:str, table_name='ETL_CONTEXT')-> None:
    '''
    Function to update the table in the respective database/schema on the context of the load

    Parameters:
    conn: Snowflake connection parameter
    NAME: Name of the data load module currently under progress
    CONT: Context value to update to be updated in the table
    table_name: Name of the default ETL_CONTEXT table to be used
    '''
    inj_time = dt.strftime(dt.now(),'%Y-%m-%d %H:%M:%S')
    if conn:
        cur = conn.cursor()

        try:
            sql = f'CREATE TABLE IF NOT EXISTS {table_name} (NAME TEXT, CONTEXT TEXT, LAST_UPDATE TEXT)'
            cur.execute(sql)
            response = cur.fetchall()

            if response[0][0].strip().upper() in (f'TABLE {table_name} SUCCESSFULLY CREATED.', 'ETL_CONTEXT ALREADY EXISTS, STATEMENT SUCCEEDED.'):
                sql = f"SELECT * FROM {table_name} WHERE NAME = '{NAME}'"
                response = cur.execute(sql).fetchall()

                if len(response) == 0:
                    sql = f"INSERT INTO {table_name } VALUES {(NAME,CONT,inj_time)}"
                    response = cur.execute(sql).fetchall()
                    logger.info(f"Insert action for row with name {NAME} and context {CONT} successfully finished")
                else:
                    sql = f"UPDATE {table_name} SET CONTEXT = '{CONT}',LAST_UPDATE = '{inj_time}'  WHERE NAME = '{NAME}'"
                    response = cur.execute(sql).fetchall()
                    logger.info(f"Update action for row with name {NAME} and context value {CONT} successfully finished")
            cur.close()
            return response
        except Error as e:
            logging.error("Following error has occured during execution %s", traceback.format_exc())
    
    else:
        logger.error("Connection variable is None")
        raise TypeError

def read_context(conn, NAME, table_name='ETL_CONTEXT', default_context='0'):
    '''
    Function to read the context from the dedicated table in the schema

    Parameters:
    conn: Snowflake Connection parameter
    NAME: Name of the data load module currently in progress
    table_name: Name of the default ETL_CONTEXT table to be used
    '''

    if conn:
        cur = conn.cursor()

        try:
            sql = f"SELECT DISTINCT CONTEXT FROM {table_name} where NAME = '{NAME}'"
            response = cur.execute(sql).fetchall()
        except ProgrammingError:
            response = []
        
        cur.close()
        try:
            if len(response) > 0:
                if len(response[0])==1:
                    context = response[0][0]
                    logging.info(f"Context value of {context} succesfully read from table {table_name}")
                    return context
            elif len(response) == 0:
                context = default_context
                logging.info(f"Context value not available in the table {table_name}. Setting it to default")
                return context
        except ProgrammingError as e:
            logging.error("Following error has occured during execution %s", traceback.format_exc())
    else:
        logger.error("Connection variable is None")
        raise TypeError



def select_distinct(conn, database, table_name, col_list=['INGESTION_DATE'], temp_table_name = 'TEMP_DIS_STAGE'):
    '''
    Function to carry out the select distinct operation on loaded tables after orchestration

    Parameters:
    conn: Snowflake connection parameter
    database: The name of the database on which the operation needs to be carried
    table_name: The name of the table on which the operation needs to be carried
    col_list: List of Columns that needs to be ungrouped
    temp_table_name: Name of the temporary staging table
    '''
    if conn:
        cur = conn.cursor()
        columns = show_columns(conn, database=database, table_name=table_name)
        # Get column names 
        columns = [each for each in columns if each.upper() not in col_list]

        # Join column names into csv string and append other query keywords
        column_str = ",".join(columns)
        sql_string = f'''
            SELECT DISTINCT {column_str}, 
                MIN({col_list[0]}) as {col_list[0]}
                FROM {table_name}
                GROUP BY {column_str}             
        '''

        # Create staging table table
        try:
            sql = f"CREATE OR REPLACE TABLE {temp_table_name} AS {sql_string}"
            response = cur.execute(sql).fetchall()
            logger.debug(f" Temporary stage table {temp_table_name} created with distinct data from {table_name}")
        except ProgrammingError as e:
            # If table already exists
            sql = "DROP TABLE IF EXISTS {temp_table_name}"
            response = cur.execute(sql).fetchall()
            sql = f"CREATE OR REPLACE TABLE {temp_table_name} AS {sql_string}"
            response = cur.execute(sql).fetchall()
            logger.debug(f" Temporary stage table {temp_table_name} dropped and created with distinct data from {table_name}")

        # Truncate original table and insert distinct rows from temporary stage
        if response[0][0].upper() == f'TABLE {temp_table_name} SUCCESSFULLY CREATED.':
            sql = f"TRUNCATE TABLE IF EXISTS {table_name}"
            response = cur.execute(sql).fetchall()
            logger.debug("Permenant table successfully truncated")

            if response[0][0].upper() == f'STATEMENT EXECUTED SUCCESSFULLY.':
                response = insert_into(
                            conn=conn,
                            to_table_name=table_name,
                            from_table_name=temp_table_name,
                            database=database )
                logger.debug(f"Insert from temporary stage table to permenant successfully {response}")

        # Drop temporary stage table
        try:
            drop_table(
                conn=conn, 
                table_name=temp_table_name
                )
        except:
            raise
        return None
    else:
        logger.error("Connection variable is None")
        raise TypeError
