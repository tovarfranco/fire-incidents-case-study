import pandas as pd
from common.utils.yaml import get_yaml
from common.utils.logger import init_logging
from common.extractor.csv import SourceCSV
from common.extractor.api import SourceApi
from common.utils.database import connect
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd

if __name__ == "__main__":
    
    logger = init_logging()
    logger.info("-------Process Start:")
    
    yaml_path = 'config/source.yaml'
    config = get_yaml(yaml_path)
    
    if config['source'] == 'csv':
        client = SourceCSV(config['path'],logger)
        df_incidents = client.read()   
    elif config['source'] == 'api':
        url = config['api']['url']
        user = config['api']['user']
        password = config['api']['password']
        client = SourceApi(url,user,password)
        df_incidents = client.read()
    else:
        logger.warning("Provide a source location")
        
    print(len(df_incidents.columns))
    database_config = get_yaml('config/database.yaml')
    conn = connect(database_config['connection'], logger)