from src.common.utils.yaml import load_yaml
from src.common.utils.logger import init_logging
from src.common.spark.extractor.source import SourceSpark
from src.common.spark.loader.bulkdata import SparkBulkData
from src.common.spark.utils import *

from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.functions import date_format, lit, to_timestamp


def main(spark, partitions_n, logger):
    
    logger.info("--------------Reading Config yaml file from s3")
    config_file = 's3://fire-incidents-config-dev/config/raw_refined.yaml'
    config = load_yaml(config_file)
    
    if config['source'] == 'csv':
        client = SourceSpark(config['csv']['path'],spark,logger)
        df_incidents = client.read_csv()
        # print(df_fire_incidents.printSchema())

        logger.info("--------------Adding Partition Column")
        df_partitioned = add_partition_cols(partition_date=config['csv']['partition_date'], df=df_incidents)
        # print(df_fire_incidents.printSchema())

        logger.info("--------------Fixing formats issues")
        df_fire_incidents = transform_columns(df_partitioned)
        # print(df_fire_incidents.printSchema())
        
        logger.info("--------------Writing data to Refined Bucket")

        bulk_data = SparkBulkData(logger,df_fire_incidents,spark,config)
        bulk_data.to_s3()   
        
        logger.info("-------Process Finished")
    # elif config['source'] == 'api':
    #     url = config['api']['url']
    #     user = config['api']['user']
    #     password = config['api']['password']
    #     client = SourceApi(url,user,password)
    #     df_incidents = client.read()
    else:
        logger.warning("-------Provide a source location")
        
    

if __name__ == "__main__":
    
    logger = init_logging()
    logger.info("-------Process Start:")
    
    spark = SparkSession.builder.appName("getdata_spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    executors_n = spark.sparkContext.getConf().get("spark.executor.instances")
    cores_n = spark.sparkContext.getConf().get("spark.executor.cores")
    
    
    if executors_n and cores_n:
        partitions_n = (int(executors_n) - 1) * int(cores_n)
    else:
        partitions_n = 4

    # logger.info(f"Spark Config Properties: \n{spark.sparkContext.getConf().toDebugString()}")
    main(spark, partitions_n, logger)
    
    