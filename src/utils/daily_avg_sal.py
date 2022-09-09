from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import when
  
def get_daily_avg_sal(df_spark: DataFrame, sal_avg: int, sal_freq_col: str) -> DataFrame :
    """
    Normalize the avarage salary to daily irrspective of any Salary Frequency
    # df_spark : Input spark dataframe 
    # sal_avg : Avg salary used for normalizing to daily level
    """
    #Normalizing the salary to a frequency of Daily
    df_spark = df_spark.withColumn("daily_salary_avg", when(df_spark[sal_freq_col] == "Annual",df_spark[sal_avg]/(12*22.00))\
                             .when(df_spark[sal_freq_col] == "Hourly",df_spark[sal_avg]*8.00)\
                             .otherwise(df_spark[sal_avg]))
    return df_spark