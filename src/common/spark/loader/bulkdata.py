import datetime
import pyspark.sql.functions as F
from awsglue import DynamicFrame

class SparkBulkData:
    def __init__(self, logger, df, spark, config):
        self.logger = logger
        self.df = df
        self.spark = spark
        self.config = config

    def to_s3(self):
        
        if self.df is not None:
            date_load = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"--------Writing table: {self.config['csv']['path']}, to bucket path: {self.config['csv']['s3_path']}")

            (
                self.df.repartition(*self.config['csv']['partitions'])
                .withColumn("date_load", F.to_timestamp(F.lit(f"{date_load}")))
                .write.mode("overwrite")
                .partitionBy(self.config['csv']['partitions'])
                .parquet(self.config['csv']['s3_path'])
            )
        else:
            self.logger.info("Dataframe is Empty, Insertion step Skipped.")