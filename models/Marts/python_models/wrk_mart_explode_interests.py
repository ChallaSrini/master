import pandas as pd
from pyspark.sql.functions import split, explode

def model(dbt, session):

    dbt.config(
     http_path="/sql/protocolv1/o/7201045960613942/0313-140603-sazhiqtq",
     materialized="table"
    )  
    

    df = dbt.source("my_src","src_lkp_exports")

    df_transformed = df.withColumn("interest", explode(split(df["interest"], ", "))).toPandas()


    return session.createDataFrame(df_transformed)