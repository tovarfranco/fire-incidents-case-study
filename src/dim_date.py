from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F
from pyspark import SparkConf

from src.common.utils.logger import init_logging
from src.common.utils.yaml import load_yaml

from pyspark.sql.functions import month
from pyspark.sql.functions import sequence, to_date, explode, col, year, month, quarter, to_timestamp, date_format


def main(spark,glueContext,logger):
    
    config_file = 's3://fire-incidents-config-dev/config/stg_dwh.yaml'
    config = load_yaml(config_file)
    df = spark.createDataFrame([(1,)], ["id"])
    
    df = df.withColumn(
        "date", 
        F.explode(F.expr("sequence(to_date('2002-01-01'), to_date('2021-12-31'), interval 1 day)"))
    )
    df =   (
                df.withColumn('year',year(df.date))
                .withColumn('month',month(df.date))
                .withColumn('quarter',quarter(df.date))
                .withColumn("date",to_timestamp(col("date")))
                .withColumn("d_o_w", date_format(col("date"), "E"))
                .withColumn("d_o_m", date_format(col("date"), "d"))
                .withColumn("d_o_y", date_format(col("date"), "D"))
            )

    df = df.drop("id")
    
    if config['source'] == 'redshift':
        AWS_IAM_ROLE = config['redshift']['role']
        DATABASE = config['redshift']['database']
        DBTABLE_TARGET = config['dim_date']     
            
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