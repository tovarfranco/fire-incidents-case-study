import psycopg2
from common.utils.logger import init_logging

def connect(params_dic, logger):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        logger.info('-----------Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)
        sys.exit(1) 
    logger.info('-----------Connection successful')
    return conn
