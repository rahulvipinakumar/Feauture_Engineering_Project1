from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType,BooleanType,DateType
  
    
def remove_special_chars(df_spark: DataFrame, col_name: str) -> DataFrame :
    """
    Remove special characters from column values
    # df_spark : Input spark dataframe 
    # col_name : Column value need to be cleaned
    """
    # Cleaning column col_name by reamoving special characters
    df_spark=df_spark.withColumn(col_name, f.regexp_replace(df_spark[col_name], "[^a-zA-Z0-9. ]", ""))
    return df_spark