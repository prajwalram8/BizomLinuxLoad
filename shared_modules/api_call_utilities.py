import os
import logging
import requests
import configparser
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

# Configuration initialization
config = configparser.ConfigParser()
config.read(os.path.join('shared_modules', 'config.ini'))


# CONFIGURATION GLOBALS
USER = config['BIZOM']['USERNAME']
PWD = config['BIZOM']['PASSWORD']


## Logging
# create logger with '__main__'
logger = logging.getLogger(__name__)

#################################################################
################### API CALL RELATED UTILITIES ##################
#################################################################

def get_access_token(BASE_URL,URL_EXT = "/oauth/directLogin/xml", username=USER, pwd=PWD):
    '''
    Function to get access token for further API calls

    Parameters:
    BASE_URL: Base URL of the API endpoint
    URL_EXT: URL extension of the API enpoint
    username: Username for the access token generator
    pwd: Password for the access toekn generator
    '''
    try:
        url = urljoin(BASE_URL,URL_EXT)
        xml = f"""
        <User>
        <username>{username}</username>
        <password>{pwd}</password>
        </User>
        """
        headers = {} # set what your server accepts
        response = requests.post(url, data=xml, headers=headers)
        root = ET.fromstring(response.content)
        xml_dict = {child.tag: child.text for child in root}
        return xml_dict['Token']
    except Exception as e:
        logging.error("The following error has occured {e}", exc_info=True)
        raise e


def create_url(BASE_URL, URL_EXT, args=None,kwargs=None):
    '''
    Function to create project specific urls

    Parameters
    BASE_URL: Base url that needs to be called
    URL_EXT: URL extensions for relevant api endpoints
    *args: list object containing URL elements
    *kwargs: dictonary object containg URL elements and associated keys
    '''
    url_bits = None
    
    # Adding url bits if exists
    if kwargs:
        url_bits = "/".join([f"{key}:{value}" for key, value in kwargs.items()]) 
    
    if args:
        url_bits =  "/".join(args)

    # Primary join with the extensions
    url = urljoin(BASE_URL, URL_EXT)

    # Secondary joins if any
    if url_bits:
        url = urljoin(url, url_bits)
    return url


def execute_api(URL, type='GET', params={}, headers={}):
    '''
    Function to execute the API

    Parameters:
    URL: URL that needs to be called
    type: Type of HTTP call
    params: Any parameters that needs to passed during the call
    headers: Any headers that may need to be passed in the call
    '''
    try:
        return requests.request(type, URL, headers=headers, params=params)
    except Exception as e:
        logger.error(f'Following exception {e} as occured', exc_info=True)
        return None
