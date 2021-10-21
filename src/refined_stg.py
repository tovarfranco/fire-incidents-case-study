from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F
from pyspark import SparkConf

from src.common.utils.logger import init_logging
from src.common.spark.utils import *

from src.common.utils.yaml import load_yaml
from src.common.spark.extractor.source import SourceSpark


# def generate_md5(df, hash_col_name, include_cols=None, exclude_cols=None, sep="|"):
#     if exclude_cols:
#         cols = [x for x in df.columns if x not in exclude_cols]
#     elif include_cols:
#         cols = include_cols
#     else:
#         cols = df.columns

#     df_result = df.withColumn(hash_col_name, F.md5(F.concat_ws(sep, *cols)))

#     return df_result

def main(spark,glueContext,logger):
    
    logger.info("--------------Reading Config yaml file from s3")
    config_file = 's3://fire-incidents-config-dev/config/refined_stg.yaml'
    config = load_yaml(config_file)

    if config['source'] == 'parquet':
        client = SourceSpark(config['parquet']['path'],spark,logger)
        df_incidents = client.read_parquet()
        
        # print(df_incidents.printSchema())

        AWS_IAM_ROLE = config['redshift']['role']
        DATABASE = config['redshift']['database']
        
        for table in config['tables']:
            
            name = table['name']
            
            logger.info(f"--------------Generating table: {name}")

            if table['md5']:
                logger.info("--------------Creating MD5")

                for md5 in table['md5']:
                    df_incidents = generate_md5(df=df_incidents,hash_col_name=md5['name'],include_cols=md5['cols'])
            
            # print(df_incidents.printSchema())
       
            if table['include_cols']:
                cols_target = table['include_cols']
            else:
                cols_target = list(set(df_incidents.columns)-set(table['exclude_cols']))
            
            logger.info(f"--------------Selecting columns for table: {name}")

            df = df_incidents.select(cols_target).dropDuplicates(cols_target)
            
            # print(df.printSchema())            
            
            DBTABLE_SOURCE = f"{config['redshift']['schema_prev']}.{name}"
            DBTABLE_TARGET = f"{config['redshift']['schema']}.{name}"
            
            pk = table['pk']
            preactions = f"truncate table {DBTABLE_SOURCE};"
            postactions = f"""begin;
                              delete from {DBTABLE_TARGET} 
                              using {DBTABLE_SOURCE} 
                              where {DBTABLE_SOURCE}.{pk} = {DBTABLE_TARGET}.{pk} ; 
                              insert into {DBTABLE_TARGET} select * from {DBTABLE_SOURCE}; end;"""
            
            rs_conn_options = {
                "dbtable": DBTABLE_SOURCE,
                "database": DATABASE,
                "aws_iam_role": AWS_IAM_ROLE,
                "preactions": preactions,
                "postactions": postactions
            }
            # WRITE TO REDSHIFT
            logger.info(f"--------------Writing table: {DBTABLE_SOURCE} to database: {DATABASE} ")

            glueContext.write_dynamic_frame.from_jdbc_conf(
                frame = DynamicFrame.fromDF(df, glueContext, "redshift"),
                catalog_connection = 'redshift-connection',
                connection_options = rs_conn_options,
                redshift_tmp_dir = f"s3://fire-incidents-config-dev/redshift/temp/{DBTABLE_TARGET}"
            )           
        
        logger.info("Process Finished")
    else:
        logger.warning("Provide a source location")  
                 

if __name__ == "__main__":
    
    logger = init_logging()
    logger.info("-------Process Start:")
    
    spark_conf = SparkConf()
    spark_conf.set("spark.sql.adaptive.enabled", "true")

    sc = SparkContext(conf=spark_conf).getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    main(spark,glueContext,logger)