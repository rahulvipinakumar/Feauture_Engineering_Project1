from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType,BooleanType,DateType
  

def one_hot_encoding(df_spark: DataFrame, col_name: str) -> DataFrame :
    """
    Populating Features for converting Categorical Features to multiple Numerical Features based on unique values in Categorical Feature
    # df_spark : Input spark dataframe 
    # col_name : Column value need to be encoded
    """
    # fetching unique column values
    unique_vals =[i[col_name] for i in df_spark.select(col_name).distinct().collect()]
    #Assigning 1 for new column based of values of col_name
    for j in unique_vals:
        df_spark = df_spark.withColumn(j, when(df_spark[col_name] == j,1).otherwise(0))
    return df_spark