import pandas as pd
from pyspark.sql.functions import split, explode

def model(dbt, session):

    dbt.config(
          materialized="table"
    )  
    

    df = dbt.source("my_src","src_lkp_exports")

    df_transformed = df.withColumn("interest", explode(split(df["interest"], ", "))).toPandas()


    return session.createDataFrame(df_transformed)