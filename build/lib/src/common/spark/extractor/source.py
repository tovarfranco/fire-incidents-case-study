from src.common.utils.logger import init_logging
from pyspark.sql import *
import pyspark.sql.functions as F

class SourceSpark:
    def __init__(self,path,spark,logger):
        self.path = path
        self.spark = spark
        self.logger = logger
    
    def read_csv(self):
        self.logger.info('--------------Reading from CSV File')
        df = self.spark\
            .read\
            .option("inferSchema","true")\
            .option("header","true")\
            .csv(self.path)
        return df
    
    def read_parquet(self):
        self.logger.info('--------------Reading from S3 Parquet')
        df =  self.spark.read.parquet(self.path)
        return df