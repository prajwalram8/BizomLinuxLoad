import os
import sys
import time
import shutil
import pathlib
import logging
import tempfile
import traceback
import configparser
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta as td
from shared_modules.email_utilities import send_mail, send_mail_update
from shared_modules.response_utilities import find_json_objects, dataframarize, json_to_df
from shared_modules.datetime_utilities import dt_to_string, string_to_dt, daterange
from shared_modules.api_call_utilities import  create_url, get_access_token,execute_api
from shared_modules.ops_utilities import check_directory_exists, terminater, add_inj_date, check_update, log_to_df, merge_in_path 
from shared_modules.snw_cdw_handler import connect_to_db, est_connection, orchestrate, logs_to_stage, file_exists_stage, context_update, read_context

## INITIALIZE GLOBAL VARIABLES
NAME = 'INVOICES'

# Configuration initialization
config = configparser.ConfigParser()
config.read(os.path.join('shared_modules', 'config.ini'))

# CONFIGURATION GLOBALS
BASE_URL = config['URL']['PRODUCTION']
DEFAULT_ST_DT = config['FIXED']['START_DT']
SNW_USER = config['SNOWFLAKE']['USER']
SNW_ACC = config['SNOWFLAKE']['ACCOUNT']
SNW_PWD = config['SNOWFLAKE']['PASSWORD']
SNW_CHW = config['SNOWFLAKE']['WAREHOUSE']
SNW_USR = config['SNOWFLAKE']['USER_ROLE']
SNW_DB = config['SNOWFLAKE']['DATABASE']
SNW_SCH = config['SNOWFLAKE']['SCHEMA']
API_QY = config['SENDGRID']['API_KEY']

# OTHER GLOBALS
TEMPFILE_PATH = tempfile.gettempdir()
LOG_PATH = f'{NAME}/Logs'
CSV_FILE_PATH = f'{NAME}/CSV_LOCAL_STAGE'
INJ_LOG_PATH = f'{NAME}/CSV_LOCAL_LOGS_STAGE'
TEMP_STAGE_PATH = f'{NAME}/TEMP_STAGE'
INJESTION_DELIVERY_CONFIRMATION  = ['kdb081293@gmail.com']

# Checking and creating necessary operating directories in the temp folder of the system
LOG_PATH = os.path.join(TEMPFILE_PATH, LOG_PATH)
pathlib.Path(LOG_PATH).mkdir(parents=True, exist_ok=True)

LOG_FORMAT='%(asctime)s: %(name)s-%(funcName)s-%(levelname)s ==> %(message)s'
FORMATTER = logging.Formatter(LOG_FORMAT)

# LOGGING INITIALIZATIONS
logger = logging.getLogger(__name__)    # Will behave as the mail logger for all the auxillary loggers
logger.setLevel(logging.DEBUG)

# Defining a module level file handler
fh = logging.FileHandler(os.path.join(TEMPFILE_PATH,LOG_PATH,f'{NAME}.log'), 'w+')  #For Logs
fh.setFormatter(FORMATTER)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

# # Console handler for streaming logs into the output console
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(FORMATTER)
ch.setLevel(logging.INFO)
logger.addHandler(ch)

logger.propagate = False

#-------------------------------------------CODE BOCK DIFFERENCE----------------------------------------
# Main Orchestration code
def main_invoices() -> None:

    # Pull connection object and establish connection
    conn = connect_to_db(
                        user=SNW_USER,
                        password=SNW_PWD,
                        account=SNW_ACC,
                        session_parameter={
                            "QUERY_TAG": f"Python Orchestract Bizom API integration - {NAME}"
                            }
                        )
    est_connection(
                    conn, 
                    database=SNW_DB,
                    compute_wh=SNW_CHW,
                    schema=SNW_SCH, 
                    user_role=SNW_USR
                    )



    # Read configuration file for context
    CONTEXT = read_context(conn, NAME=NAME,default_context=DEFAULT_ST_DT)
    logger.info(f'Context variable for {NAME} API Injestion script successfully initialized')

     # Configure Local Variables
    URL_EXT = '/payments/getsales'
    START =  string_to_dt(CONTEXT).date()                  # Needs to be set up to read from he setting.json file eventually
    END = dt.now().date() + td(days=1)                     # Need to figure out a way to dynamical adjust the size
    BUFFER = 1                                             # To be defined based on the maximum timeframe within which order details can be modified in the Bizom System
    JUMP_SIZE = 200
    STATUSES = []
    STATUS = []
    STATUS2 = []
    STATUS3 = []
    TIME_OUT_MAX = 60
    LOOP_TIMEOUT = time.time() + 60*TIME_OUT_MAX
    BREAK_END = None
    PARAMS = {
            'access_token': f'{get_access_token(BASE_URL=BASE_URL)}',
            'responsetype': 'json',
            'dateType': 'modified',
        }

    
    # Defining temp folder location paths to stage the injested files
    INT_STAGE_PATH = os.path.join(
            TEMPFILE_PATH, 
            TEMP_STAGE_PATH, 
            f'{NAME}_{START}_{END}_{str(int(time.mktime(dt.now().timetuple())))}'
            )

    INT_STAGE_PATH_2 = os.path.join(
            TEMPFILE_PATH, 
            TEMP_STAGE_PATH, 
            f'{NAME}_{START}_{END}_{str(int(time.mktime(dt.now().timetuple())))}_DETAILS'
            )
    
    INT_STAGE_PATH_3 = os.path.join(
            TEMPFILE_PATH, 
            TEMP_STAGE_PATH, 
            f'{NAME}_{START}_{END}_{str(int(time.mktime(dt.now().timetuple())))}_DETAILS_2'
            )

    # Creating relevant paths
    pathlib.Path(INT_STAGE_PATH).mkdir(parents=True, exist_ok=True)
    pathlib.Path(INT_STAGE_PATH_2).mkdir(parents=True, exist_ok=True)
    pathlib.Path(INT_STAGE_PATH_3).mkdir(parents=True, exist_ok=True)

    
    # Loop to control the upload until prescribed sequence begins
    logger.info("Global and Parameters successfully initialized; entering the batch loop sequence..")
    
    REDO = True

    for each in daterange(START, END):

        if time.time() > LOOP_TIMEOUT:
            # Update END parameter to the recent day before the current value of each as the loop as not entered the data procurement pipeline 
            BREAK_END = each - td(days=1)
            REDO = False
            logger.info("Internal Break criteria achieved due to timer condition. Breaking the batch loop")

            # Break the primary date loop
            break
        
        else:
            START_SEQ  = 0
            while True:

                # Batch End
                END_SEQ = START_SEQ + JUMP_SIZE

                # Dynamic Parameters updated
                PARAMS['fromdate'] = dt_to_string(each)
                PARAMS['todate'] = dt_to_string(each)
                PARAMS['startseq'] = str(START_SEQ)
                PARAMS['endseq'] = str(END_SEQ)

                # Create URL for call
                url = create_url(BASE_URL=BASE_URL,URL_EXT=URL_EXT)

                # Call API and save the response
                response = execute_api(url, 'GET', PARAMS)
                logger.info(f"URL: {response.url}")

                # Save the relevant JSON object in response       
                jason = find_json_objects(response,)

                if not terminater(jason=jason, record_path='Payments',loop_timeout=None):
                    logger.debug("API call failed to respond with any data, iterating to the next date")
                    break
                else:
                    
                    # Save the relevant JSON object in response       
                    jason = find_json_objects(response, json_obj_name='Payments')
                    
                    # Converting JSON object into a dataframe object
                        # income Level Data 
                    try:
                        output_df = json_to_df(jason,)
                    except ValueError:
                        raise

                        # income Detail level data
                    try:
                        output_df2 =dataframarize([subeach for each in list(output_df['Paymentdetail']) for subeach in each if isinstance(subeach, dict)])
                    except ValueError:
                        raise

                        # extra details
                    try:
                        output_df3 = dataframarize([subeach for each in list(output_df['Schemedetail']) for subeach in each if isinstance(subeach, dict)])
                    except ValueError:
                        raise

                    # Preprocessing the Dataframe and saving it as a csv file
                    try:
                        output_df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
                    except ValueError:
                        output_df.replace("\n", "", inplace=True)
                    except AttributeError as e:
                        raise
                        
                    try:
                        output_df2.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
                    except ValueError:
                        output_df2.replace("\n", "", inplace=True)
                    except AttributeError as e:
                        raise

                    try:
                        output_df3.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
                    except ValueError:
                        output_df3.replace("\n", "", inplace=True)
                    except AttributeError as e:
                        raise

                    # Staging dataframes locally
                    file_path = os.path.join(INT_STAGE_PATH, f'STAGE_{NAME}_{PARAMS["fromdate"]}_{PARAMS["startseq"]}_{PARAMS["endseq"]}.csv')
                    output_df.to_csv(file_path,sep="\t", index=False, line_terminator="\n", encoding='utf-8')

                    file_path2 = os.path.join(INT_STAGE_PATH_2,  f'STAGE_{NAME}_{PARAMS["fromdate"]}_{PARAMS["startseq"]}_{PARAMS["endseq"]}_DETAILS.csv')
                    output_df2.to_csv(file_path2, sep="\t", index=False, line_terminator="\n", encoding='utf-8')

                    file_path3 = os.path.join(INT_STAGE_PATH_3,  f'STAGE_{NAME}_{PARAMS["fromdate"]}_{PARAMS["startseq"]}_{PARAMS["endseq"]}_DETAILS_2.csv')
                    output_df3.to_csv(file_path3, sep="\t", index=False, line_terminator="\n", encoding='utf-8')

                    # Update the sequence start range
                    START_SEQ = END_SEQ

    logger.info("Internal Break criteria achieved as API call conditions have reached max limit")
        
    if REDO:
        # Update context variable
        BREAK_END = each

    # Creation path variable to which the original files needs to be renamed to
    INT_STAGE_PATH_RN = os.path.join(
            TEMPFILE_PATH, 
            TEMP_STAGE_PATH, 
            f'{NAME}_{START}_{BREAK_END}_{str(int(time.mktime(dt.now().timetuple())))}'
            )

    INT_STAGE_PATH_2_RN = os.path.join(
            TEMPFILE_PATH, 
            TEMP_STAGE_PATH, 
            f'{NAME}_{START}_{BREAK_END}_{str(int(time.mktime(dt.now().timetuple())))}_DETAILS'
            )
    
    INT_STAGE_PATH_3_RN = os.path.join(
            TEMPFILE_PATH, 
            TEMP_STAGE_PATH, 
            f'{NAME}_{START}_{BREAK_END}_{str(int(time.mktime(dt.now().timetuple())))}_DETAILS_2'
            )

    # Rename the directory being stored to the new name
    os.rename(src=INT_STAGE_PATH  ,  dst=INT_STAGE_PATH_RN)
    os.rename(src=INT_STAGE_PATH_2,dst=INT_STAGE_PATH_2_RN)
    os.rename(src=INT_STAGE_PATH_3,dst=INT_STAGE_PATH_3_RN)

    if check_directory_exists(dir_path=INT_STAGE_PATH_RN) and check_directory_exists(dir_path=INT_STAGE_PATH_2_RN) and check_directory_exists(dir_path=INT_STAGE_PATH_3_RN):    
        
        # Creating merged files
        upload_df1 = merge_in_path(INT_STAGE_PATH_RN)
        upload_df2 = merge_in_path(INT_STAGE_PATH_2_RN)
        upload_df3 = merge_in_path(INT_STAGE_PATH_3_RN)
        logger.info("Merging the individual files in the temporary location for upload")
    

        # Loading the merged dataframes into snowflake
        if upload_df1.shape[0] > 0 or upload_df2.shape[0] > 0 or upload_df3.shape[0] > 0:
            if upload_df1.shape[0] > 0:
                upload_df1 = add_inj_date(upload_df1)
                STATUS = list(
                    orchestrate(conn=conn,
                        df=upload_df1, 
                        table_name=f'BIZ_{NAME}_MAIN', 
                        database=SNW_DB,
                        csv_filename=f"BIZ_{NAME}_MAIN",
                        csv_file_path=CSV_FILE_PATH,
                        data_stage=f'DATA_STAGE_{NAME}')[0]
                        )
                # Update the status to include derived data points
                STATUS = check_update(STATUS,  dt_to_string(START), dt_to_string(BREAK_END))
                # Append to master status list
                STATUSES.extend([STATUS])
        
            if upload_df2.shape[0] > 0:
                upload_df2 = add_inj_date(upload_df2)
                STATUS2 = list(
                    orchestrate(conn=conn,
                        df=upload_df2, 
                        table_name=f'BIZ_{NAME}_MAIN_DETS', 
                        database=SNW_DB,
                        csv_filename=f"BIZ_{NAME}_MAIN_DETS",
                        csv_file_path=CSV_FILE_PATH,
                        data_stage=f'DATA_STAGE_{NAME}')[0]
                        )
                # Update the status to include derived data points
                STATUS2 = check_update(STATUS2,  dt_to_string(START), dt_to_string(BREAK_END))
                # Append to master status list
                STATUSES.extend([STATUS2])

            if upload_df3.shape[0] > 0:
                upload_df3 = add_inj_date(upload_df3)
                STATUS3 = list(
                    orchestrate(conn=conn,
                        df=upload_df3, 
                        table_name=f'BIZ_{NAME}_MAIN_DETS_2', 
                        database=SNW_DB,
                        csv_filename=f"BIZ_{NAME}_MAIN_DETS_2",
                        csv_file_path=CSV_FILE_PATH,
                        data_stage=f'DATA_STAGE_{NAME}')[0]
                        )
                # Update the status to include derived data points
                STATUS3 = check_update(STATUS3,  dt_to_string(START), dt_to_string(BREAK_END))
                # Append to master status list
                STATUSES.extend([STATUS3])
                logger.info("Merged dataframe successfully uploaded into the datawarehouse")

            # Update the ingestion status based on the final status recieved
            ingestion_status = pd.DataFrame(
                            data=STATUSES, 
                            columns=['stage','status', 'rows_parsed', 'rows_loaded', 'error_limit', 'errors_seen', 
                            'first_error', 'first_error_line', 'first_error_character', 'first_error_column_name', 
                            'Load_status', 'sq_start', 'sq_end']
                            )
            # Update context variable with buffer
            CONTEXT = BREAK_END - td(days=BUFFER)
            CONTEXT = dt_to_string(CONTEXT)

            # Update the context variable (Placement here to ensure completion of all critical steps)
            context_update(conn, NAME=NAME, CONT=CONTEXT, table_name='ETL_CONTEXT')
            logger.info(f"Ingestion process for {NAME} completed; Context table successfully updated.")

            # Writing ingestion status to csv
            ING_STAT_FILE_NAME = f"ING_STAT_{NAME}_{dt_to_string(dt.now())}.csv"
            ingestion_status.to_csv(os.path.join(TEMPFILE_PATH, LOG_PATH, ING_STAT_FILE_NAME))
        else:
            ingestion_status = pd.DataFrame()
            logger.info(f"No ingestion has been carried out as merged DF has zero rows")
    else:
        ingestion_status = None
        logger.info(f"Ingestion process for {NAME} did not yield and new data; Context value remains the same.")

#--------------------------------------- CODE BOCK DIFFERENCE ENDS----------------------------------------
    
    if isinstance(ingestion_status, pd.DataFrame):
        # Send Mail
        try:
            logger.info("Sending status report to the predefined delivery list")
            send_mail(
                FILE_PATHS=[
                    os.path.join(TEMPFILE_PATH,LOG_PATH,ING_STAT_FILE_NAME), 
                    os.path.join(TEMPFILE_PATH,LOG_PATH,f'{NAME}.log')
                    ], 
                DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                FILE_NAMES=[ING_STAT_FILE_NAME, NAME], 
                NAME=NAME,
                API_QY=API_QY)
            logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
        except:
            logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")
        # Send Mail
    else:
        try:
            send_mail_update(
                DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                API_QY=API_QY,
                NAME=NAME)
            logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
        except:
            logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")


    # Removing the temporary stage directory
    shutil.rmtree(INT_STAGE_PATH_RN, ignore_errors=True)
    logger.info("Local stage directory removed successfully")
    return None

if __name__ == "__main__":
    main_invoices()

