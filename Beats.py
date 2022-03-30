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
from shared_modules.email_utilities import send_mail, send_mail_update
from shared_modules.response_utilities import find_json_objects, dataframarize 
from shared_modules.datetime_utilities import dt_to_string
from shared_modules.api_call_utilities import  create_url, get_access_token,execute_api
from shared_modules.ops_utilities import terminater, add_inj_date, check_update, merge_in_path, check_directory_exists 
from shared_modules.snw_cdw_handler import connect_to_db, est_connection,copy_into_table, context_update
## INITIALIZE GLOBAL VARIABLES
NAME = 'BEATS'

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
INJESTION_DELIVERY_CONFIRMATION  = ['kdb081293@gmail.com','lakshman.bmln@gmail.com']

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
def main_beats() -> None:

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
    logger.info('Skipping context variable read as it is a full master load for every run')

    # Configure Local Variables
    URL_EXT = '/beats/getbeats'
    START =  DEFAULT_ST_DT                          # Needs to be set up to read from he setting.json file eventually
    END = f'{dt_to_string(dt.now())}'                             # Needs to be set up to read from he setting.json file eventually
    JUMP_SIZE = 5                                 
    STATUSES = []
    STATUS = []
    STATUS2 = []
    TIME_OUT_MAX = 45
    LOOP_TIMEOUT = time.time() + 60*TIME_OUT_MAX
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

    # Creating relevant paths
    pathlib.Path(INT_STAGE_PATH).mkdir(parents=True, exist_ok=True)
    pathlib.Path(INT_STAGE_PATH_2).mkdir(parents=True, exist_ok=True)
    
    # Loop to control the upload until prescribed sequence begins
    logger.info("Global and Parameters successfully initialized; entering the batch loop sequence..")
    
    START_SEQ  = 0

    while True:

        # Batch End
        END_SEQ = START_SEQ + JUMP_SIZE

        # Dynamic Parameters updated
        PARAMS['startseq'] = str(START_SEQ)
        PARAMS['endseq'] = str(END_SEQ)
        PARAMS['fromdate'] = dt_to_string(START)
        PARAMS['todate'] = dt_to_string(END)

        # Create URL for call
        url = create_url(BASE_URL=BASE_URL,URL_EXT=URL_EXT)

        # Call API and save the response
        response = execute_api(url, 'GET', PARAMS)
        logger.info(f"URL: {response.url}")

        # Save the relevant JSON object in response       
        jason = find_json_objects(response, json_obj_name='Response')
        logger.debug("Json objects recieved and located")

        if not terminater(jason=jason, record_path='Beats',loop_timeout=LOOP_TIMEOUT):
            logger.debug("API call failed to respond with any data, iterating to the next date")
            break
        else:

            # Custom change for beats master considering its format of data collection
            true_beats=[]
            true_beats_dets = []
            for each in list(jason['Beats']):
                true_beats.append(jason['Beats'][each])
                true_beats_dets.append(jason['Beats'][each]['Beatdetails']['Beatdetail']) 

            # #flattening out beats
            true_beats_dets = [subeach for each in true_beats_dets for subeach in each]

            # Converting JSON responses to dataframe
            output_df = dataframarize(true_beats)
            output_df2 = dataframarize(true_beats_dets)

            # Staging dataframes locally
            try:
                file_path = os.path.join(INT_STAGE_PATH, f'STAGE_{NAME}_{PARAMS["fromdate"]}_{PARAMS["startseq"]}_{PARAMS["endseq"]}.csv')
                output_df.to_csv(file_path,sep="\t", index=False, line_terminator="\n", encoding='utf-8')
            except AttributeError:
                raise
        
            # Staging dataframes locally
            try:
                file_path2 = os.path.join(INT_STAGE_PATH_2,  f'STAGE_{NAME}_{PARAMS["fromdate"]}_{PARAMS["startseq"]}_{PARAMS["endseq"]}_DETAILS.csv')
                output_df2.to_csv(file_path2, sep="\t", index=False, line_terminator="\n", encoding='utf-8')
            except AttributeError:
                raise

            # Update the sequence start range
            START_SEQ = END_SEQ

    logger.info("Internal Break criteria achieved as API call conditions have reached max limit")

    # Creation path variable to which the original files needs to be renamed to
    INT_STAGE_PATH_RN = os.path.join(
            TEMPFILE_PATH, 
            TEMP_STAGE_PATH, 
            f'{NAME}_{START}_{dt_to_string(END)}_{str(int(time.mktime(dt.now().timetuple())))}'
            )

    INT_STAGE_PATH_2_RN = os.path.join(
            TEMPFILE_PATH, 
            TEMP_STAGE_PATH, 
            f'{NAME}_{START}_{dt_to_string(END)}_{str(int(time.mktime(dt.now().timetuple())))}_DETAILS'
            )

    # Rename the directory being stored to the new name
    os.rename(src=INT_STAGE_PATH, dst=INT_STAGE_PATH_RN)
    os.rename(src=INT_STAGE_PATH_2,dst=INT_STAGE_PATH_2_RN)
        
    if check_directory_exists(dir_path=INT_STAGE_PATH_RN) and check_directory_exists(dir_path=INT_STAGE_PATH_2_RN):
        # Creating merged files
        upload_df1 = merge_in_path(INT_STAGE_PATH_RN)
        upload_df2 = merge_in_path(INT_STAGE_PATH_2_RN)
        logger.info("Merging the individual files in the temporary location for upload")

        # Loading the merged dataframes into snowflake
        if upload_df1.shape[0] > 0 or upload_df2.shape[0] > 0:
            if upload_df1.shape[0] > 0:
                upload_df1 = add_inj_date(upload_df1)
                STATUS = copy_into_table(
                        conn=conn,
                        df=upload_df1,
                        csv_output_name=f"BIZ_{NAME}_MAIN",
                        csv_file_path=CSV_FILE_PATH,
                        data_stage=f'DATA_STAGE_{NAME}',
                        table_name=f'BIZ_{NAME}_MAIN',
                        _type="PERMENANT")[0][0]
                STATUS = list(STATUS)
                # Update the status to include derived data points
                STATUS = check_update(STATUS,  dt_to_string(START), dt_to_string(END))
                # Append to master status list
                STATUSES.extend([STATUS])
            
            if upload_df2.shape[0] > 0:
                if upload_df2.shape[0] > 0:
                    upload_df2 = add_inj_date(upload_df2)
                    STATUS2 = copy_into_table(
                            conn=conn,
                            df=upload_df2,
                            csv_output_name=f"BIZ_{NAME}_MAIN_DETS",
                            csv_file_path=CSV_FILE_PATH,
                            data_stage=f'DATA_STAGE_{NAME}',
                            table_name=f'BIZ_{NAME}_MAIN_DETS',
                            _type="PERMENANT")[0][0]
                    STATUS2 = list(STATUS2)
                    # Update the status to include derived data points
                    STATUS2 = check_update(STATUS2,  dt_to_string(START), dt_to_string(END))
                    # Append to master status list
                    STATUSES.extend([STATUS2])
                    logger.info("Merged dataframe successfully uploaded into the datawarehouse")

            ingestion_status = pd.DataFrame(
                    data=STATUSES, 
                    columns=['stage','status', 'rows_parsed', 'rows_loaded', 'error_limit', 'errors_seen', 
                    'first_error', 'first_error_line', 'first_error_character', 'first_error_column_name', 
                    'Load_status', 'sq_start', 'sq_end']
                    )

            #To update in the ETL context
            CONTEXT = dt_to_string(END)
        
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
    shutil.rmtree(INT_STAGE_PATH_2_RN, ignore_errors=True)
    logger.info("Local stage directory removed successfully")
    return None

if __name__ == "__main__":
    main_beats()