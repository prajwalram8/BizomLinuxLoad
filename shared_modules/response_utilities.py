import json
import logging
import pandas as pd
from xml.etree import ElementTree as ET


## Logging
# create logger with '__main__'
logger = logging.getLogger('__main__.' + __name__)

#################################################################
###################### RESPONSE  UTILITIES #####################
#################################################################
def find_xml_objects(response, xml_obj_name='Outlet'):
    '''
    Function to that identifiled relevant xml child notes from parent note within the content of a response

    Parameters
    response: Https response object from the requests lib
    xml_obj_name: name of the xml objects that needs to be extracted
    '''
    try:
        tree = ET.fromstring(response.content.strip())
        # From tree - Find all outlet tags
        xml_objects = tree.findall(xml_obj_name)
        return xml_objects
    except Exception as e:
        logger.error(f'Following exception {e} as occured', exc_info=True)
        raise


def find_json_objects(response, json_obj_name=None):
    '''
    Function to that identifiled relevant json key from parent key within the json content of a response

    Parameters
    response: Https response object from the requests lib
    json_obj_name: name of the json objects that needs to be extracted
    '''
    try:
        if json_obj_name:
            jason = response.json()
            # From tree - Find all outlet tags
            jason = jason.get(json_obj_name)
            return jason
        else:
            return response.json()
    except Exception as e:
        logger.error(f'Following exception {e} as occured', exc_info=True)
            

def json_to_df(jason,rec_path=None):
    '''
    This function converts a json oject into a dataframe

    Parameters
    jason: Json Object
    rec_path: A list object containing the path of the records
    '''
    try:
        if rec_path:
            return pd.json_normalize(jason, record_path=rec_path)
        else:
            return  pd.json_normalize(jason)
    except Exception as e:
        logger.error(f'Following exception {e} as occured', exc_info=True)
        return None


def json_file_to_df(path):
    '''
    Function to convert logs stored in json format in the path "tmp/{module_name}/Logs into dataframe for upload into CDW

    Parameters:
    path: Path to the log files location
    '''
    try:
        dict_list = []
        with open(path,"r") as opened:
            dict_list=opened.readlines()
        dict_list = map(lambda x: json.loads(x), dict_list)
        log_df  = pd.DataFrame(dict_list)
        return log_df
    except Exception as e:
        logger.error(f'Following exception {e} as occured', exc_info=True)


def dataframarize(obj:list)->pd.DataFrame:
    '''
    Function to convert a list object into a dataframe

    Parameters:
    obj: List object that is a list of lists
    '''
    try:
        df = pd.DataFrame(obj)
        logger.debug("Data Frame successfully created")
        return df
    except ValueError:
        raise ValueError
    except Exception as e:
        logger.error(f'Following exception {e} as occured', exc_info=True)


