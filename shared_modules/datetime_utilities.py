import logging
from datetime import datetime as dt
from datetime import timedelta as td


## Logging
# create logger with '__main__'
logger = logging.getLogger('__main__.' + __name__)

#################################################################
###################### DATETIME UTILITIES #####################
#################################################################
def dt_to_string(object):
    '''
    Convert date object to string

    Parameter:
    object: datetime object that needs to be converted
    '''
    try:
        if isinstance(object, str):
            return object
        return dt.strftime(object, '%Y-%m-%d')
    except Exception as e:
        logger.error(f'Following exception {e} as occured \n', exc_info=True)
        return None
        

def string_to_dt(string):
    '''
    Convert date string to datetime object

    Parameter:
    string: string object that needs to be converted
    '''
    try:
        return dt.strptime(string, '%Y-%m-%d')
    except Exception as e:
        logger.error(f'Following exception {e} as occured \n', exc_info=True)
        return None


def date_jump(start_date:dt.date, end_date:dt.date, interval = 10)->dt.date:
    '''
    Generator created to return dates withing a specified date range

    Parameters:
    start_date: start date range
    end_date: end date range
    '''
    days = (end_date - start_date).days
    jumps = days//interval
    walks = days%interval

    jump_list = []
    for jump in range(jumps):
        temp_end_dt = start_date + td(days=interval)
        jump_list.append((dt_to_string(start_date), dt_to_string(temp_end_dt)))
        start_date = temp_end_dt
    
    walk_list = []
    for walk in range(walks):
        temp_end_dt =start_date + td(1)
        walk_list.append((dt_to_string(start_date), dt_to_string(start_date)))
        start_date = temp_end_dt

    for date_range in  [subeach for each in [jump_list,walk_list] for subeach in each]:
        yield date_range


def daterange(start_date:dt.date, end_date:dt.date)->dt.date:
    for n in range(int((end_date - start_date).days)):
        yield start_date + td(n)



def step_daterange(start_date:dt.date, end_date:dt.date, step_size=10):
    range_list = []
    
    ran = end_date - start_date
    ran = int(ran.days)
    steps = ran // step_size
    skips = ran % step_size
    
    for step in range(steps):
        dt_start = start_date
        dt_end =  start_date + td(days=step_size)
        start_date = dt_end 
        range_list.append((dt_to_string(dt_start), dt_to_string(dt_end)))
        
    for skip in range(skips+1):
        dt_start = start_date
        dt_end =  start_date
        start_date = dt_end + td(days=1)
        range_list.append((dt_to_string(dt_start), dt_to_string(dt_end)))

    return range_list

