from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType,BooleanType,DateType
  
def get_year_month(df_spark: DataFrame,date_col: str, year_col: str,month_col: str) -> DataFrame :
    """
    Used to extract Year and Month from given date in that column
    # df_spark : Input spark dataframe 
    # sal_avg : Avg salary used for normalizing to daily level
    """
    # column with Year and month details of date_col
    df_spark=df_spark.withColumn(year_col,f.year(f.to_timestamp(df_spark[date_col],'yyyy-MM-dd hh24:mi:ss.ff3'))\
                               .cast(IntegerType())).withColumn(month_col,\
                                                                f.month(f.to_timestamp(df_spark[date_col],'yyyy-MM-dd hh24:mi:ss.ff3'))\
                                                                .cast(IntegerType()))
    return df_spark