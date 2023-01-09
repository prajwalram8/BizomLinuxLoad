import os
import glob
import json
import time
import logging
import traceback
import pandas as pd
from pandas.errors import EmptyDataError
from pytz import timezone
from datetime import datetime as dt



## Logging
# create logger with '__main__'
logger = logging.getLogger('__main__.' + __name__)

#################################################################
################# OPERATIONS RELATED UTILITIES ##################
#################################################################

def terminater(jason:dict, record_path:str, loop_timeout:time.time, prim_obj=None)-> bool:
    '''
    This function takes an jason input (typically the body of the response) and check for clues that indicate no data in the response

    Parameters:
    jason: JSON object that needs to be checked
    '''

    # This section checks if for nested condition 
    if prim_obj:
        jason = jason[prim_obj]

    # Check for condition 1, where the result key of the will be used
    try:
        condition_result = bool(jason['Result'])
    except (KeyError, TypeError) as e:
        try:
            condition_result = bool(jason['result'])
        except (KeyError, TypeError) as e:  #if the there is no response or no key for the second case, automatically assigning it to False
            condition_result = False
        except:
            logger.error("uncaught exception: %s", traceback.format_exc())

    # Check for presence of information in the record path
    try:
        condition_rec_path = jason[record_path].strip() != ''
    except (TypeError, AttributeError, KeyError) as e:
        try:
            condition_rec_path = (len(jason[record_path]) != 0)
        except (TypeError, AttributeError, KeyError) as e:
            condition_rec_path = True
        except :
            logger.error("uncaught expception: %s", traceback.format_exc())
    
    if loop_timeout:
        condition_timeout = not time.time() > loop_timeout
    else:
        condition_timeout = True

    logger.debug(f"Conditions for the terminators are {condition_result} AND {condition_rec_path} AND {condition_timeout}")

    # Both conditions check
    if  (condition_result and condition_rec_path and condition_timeout):
        return True
    else: 
        return False


def add_inj_date(df,zone="Asia/Dubai"):
    '''
    Function to add current date as injestion date into a dataframe

    Parameters:
    df: Dataframe to which the injestion date needs to be added 
    '''
    tz = timezone(zone)
    with_timezone = tz.localize(dt.now())
    df['Ingestion_date'] = dt.strftime(with_timezone, "%Y-%m-%d %H:%M:%S %z")
    return df


def check(stat:list) -> bool:
    '''
    Function to take a status list to check for load status of the table

    Parameters:
    stat: List output of the status after a load job
    '''
    try:
        if len(set(stat[2:4])) != 1:
            return False
        return True
    except Exception as e:
        logger.info(f"The following exception has occured {e} \n", exc_info=True)



def check_update(ls:list, START:int,END:int) -> list:
    '''
    Function to take a list as a and extend values based on on the check variable

    Parameters:
    ls: List input
    START: Start value to be extended with
    END: End value to be extended with
    '''
    try:
        if check(ls):
            ls.extend(['Successful'])
            ls.extend([START, END])
        else:
            ls.extend(['Partially Successful'])
            ls.extend([START, END])
        return ls
    except Exception as e:
        logger.info(f"The following exception has occured {e} \n", exc_info=True)


def log_to_df(path):
    try:
        dict_list = []
        with open(path,"r") as opened:
            dict_list=opened.readlines()
        dict_list = map(lambda x: json.loads(x), dict_list)
        log_df  = pd.DataFrame(dict_list)
        return log_df
    except Exception as e:    
        logger.info(f"The following exception has occured {e} \n", exc_info=True)


# UDF
def merge_in_path(path):
    all_files = glob.glob(os.path.join(path, "*.csv"))

    all_df = []
    for f in all_files:
        try:
            df = pd.read_csv(f, sep='\t')
        except EmptyDataError:
            continue
        df['file'] = f.split('/')[-1]
        all_df.append(df)

    try:
        merged_df = pd.concat(all_df, ignore_index=True, sort=True)
    except ValueError:
        try:
            merged_df = all_df[0]
        except IndexError:
            merged_df = pd.DataFrame()
    
    return merged_df


def check_directory_exists(dir_path):
    if os.path.isdir(dir_path):
        if not os.listdir(dir_path):
            return False
        else:
            return True
    else:
        print("Given directory doesn't exist")
        False
        
