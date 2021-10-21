from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import date_format, lit, to_timestamp

def add_partition_cols(partition_date,df):
        return df.withColumn("year", date_format(partition_date, "y").cast("int")).withColumn("month", date_format(partition_date, "MM").cast("int")).withColumn("day", date_format(partition_date, "dd").cast("int"))
    
def transform_columns(df):
        return df.toDF(*[c.replace(" ", "_").lower() for c in df.columns])


def generate_md5(df, hash_col_name, include_cols=None, exclude_cols=None, sep="|"):
    if exclude_cols:
        cols = [x for x in df.columns if x not in exclude_cols]
    elif include_cols:
        cols = include_cols
    else:
        cols = df.columns

    df_result = df.withColumn(hash_col_name, F.md5(F.concat_ws(sep, *cols)))

    return df_result
