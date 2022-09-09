from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as f

def gap_filling_by_mode(df_spark: DataFrame, grp_by_cols: list, req_col: str) -> DataFrame :
    """
    Used for Categorical Imputation for filling missing values in categorical columns
    # df_spark : Input spark dataframe where missing data need to be populated
    # grp_by_cols : List of columns used for finding the mode of column values, whose null values need to be replaced
    # req_col : Column whose null values need to be replaced
    """
    # Creating copy of column lists used for grouping
    grp_by_cols_copy=grp_by_cols.copy()
    grp_by_cols_copy.append(req_col)
    # Finding Mode values of req_col for each group of grp_by_cols values
    df_spark_tmp=df_spark.groupby(grp_by_cols_copy).count().orderBy("count", ascending=False)
    windowSpec  = Window.partitionBy(grp_by_cols).orderBy(f.col("count").desc())
    df_spark_tmp_final=df_spark_tmp.withColumn("rank",f.row_number().over(windowSpec))
    # Rank=1 is used for finding the most frequently used value of req_col(mode) for each combinations of grp_by_cols
    df_spark_tmp_final=df_spark_tmp_final.filter(f.col("rank") == 1).withColumnRenamed(req_col,req_col+"_new")  
    #Collecting the column list of input dataframe
    main_cols=df_spark.columns
    #Joining the Mode dataframe and input dataframe
    df_spark=df_spark.join(df_spark_tmp_final, on=grp_by_cols, how='left')
    #Replacing NULL values of req_col with Mode value(req_col_new)
    df_spark=df_spark.withColumn(req_col,f.coalesce(f.col(req_col),f.col(req_col+"_new")))
    return df_spark[[main_cols]]
