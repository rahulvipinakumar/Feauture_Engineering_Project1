from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType,BooleanType,DateType

    
def drop_feauture(df_spark: DataFrame,col_list: list) -> DataFrame :
    """
    Used to remove features from the input dataframe
    # df_spark : Input spark dataframe 
    # col_list : LIST of columns to be removed
    """
    # Removing columns in the given list
    for i in col_list:
        df_spark=df_spark.drop(i)
    return df_spark