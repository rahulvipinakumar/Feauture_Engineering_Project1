from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType,BooleanType,DateType,StringType
import pyspark.sql.functions as f
  
def get_avg_sal(df_spark: DataFrame, sal_from: int, sal_to: int) -> DataFrame :
    """
    Used to get average of two salary columns which is in params
    # df_spark : Input spark dataframe 
    # sal_from and sal_to are salary ranges for each job positions
    """
    # Will convert the salary columns to Integer type for performing aggregate functions
    df_spark = df_spark.withColumn(sal_from,df_spark[sal_from].cast(IntegerType()))
    df_spark = df_spark.withColumn(sal_to,df_spark[sal_to].cast(IntegerType()))
    
    # Finding average salary from from and To salaries
    df_spark = df_spark.withColumn("salary_avg",(df_spark[sal_from]+df_spark[sal_to])/2.0)
    return df_spark
