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


def main(spark,glueContext,logger):
    
    config_file = 's3://fire-incidents-config-dev/config/stg_dwh.yaml'
    config = load_yaml(config_file)

    if config['source'] == 'redshift':
        AWS_IAM_ROLE = config['redshift']['role']
        DATABASE = config['redshift']['database']
        
        for dim in config['dim']:
            
            my_conn_options = {
                "url": config['redshift']['endpoint'],
                "dbtable": dim["source"],
                "user": "franco",
                "password": "Tabitha481!",
                "redshiftTmpDir": f"s3://fire-incidents-config-dev/redshift/temp/{dim['source']}",
                "aws_iam_role": AWS_IAM_ROLE,
            }
            
            dyf = glueContext.create_dynamic_frame_from_options("redshift", my_conn_options)
            df = dyf.toDF()
                        
            print(df.printSchema())
            
            DBTABLE_TARGET = dim['target']
            
            rs_conn_options = {
                "dbtable": DBTABLE_TARGET,
                "database": DATABASE,
                "aws_iam_role": AWS_IAM_ROLE
            }
            # WRITE TO REDSHIFT
            glueContext.write_dynamic_frame.from_jdbc_conf(
                frame = DynamicFrame.fromDF(df, glueContext, "redshift"),
                catalog_connection = 'redshift-connection',
                connection_options = rs_conn_options,
                redshift_tmp_dir = f"s3://fire-incidents-config-dev/redshift/temp/{DBTABLE_TARGET}"
            )           


        fact_config = config['fact']
        for source in fact_config['sources']:
            
            my_conn_options = {
                    "url": config['redshift']['endpoint'],
                    "dbtable": source["name"],
                    "user": "franco",
                    "password": "Tabitha481!",
                    "redshiftTmpDir": f"s3://fire-incidents-config-dev/redshift/temp/dwh/{source['name']}",
                    "aws_iam_role": AWS_IAM_ROLE,
                }
            
            dyf = glueContext.create_dynamic_frame_from_options("redshift", my_conn_options)
            df = dyf.toDF()
            df.createOrReplaceTempView(source['view'])
        
        df = spark.sql(fact_config['query'])
        
        if fact_config['include_cols']:
            cols_target = fact_config['include_cols']
        else:
            cols_target = list(set(df.columns)-set(fact_config['exclude_cols']))
        
        df = df.select(cols_target).dropDuplicates(cols_target)
            
        DBTABLE_TARGET = fact_config['target']     
            
        rs_conn_options = {
            "dbtable": DBTABLE_TARGET,
            "database": DATABASE,
            "aws_iam_role": AWS_IAM_ROLE
        }
        # WRITE TO REDSHIFT
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