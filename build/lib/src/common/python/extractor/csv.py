import pandas as pd
from common.utils.logger import init_logging

class SourceCSV:
    def __init__(self,path,logger):
        self.path = path
        self.logger = logger
    
    def read(self):
        self.logger.info('--------------Reading from CSV File')
        df = pd.read_csv(self.path, low_memory=False)
        return df