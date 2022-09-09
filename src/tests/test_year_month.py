from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType,StringType
from utils.year_month import get_year_month

def spark_session():
    """Fixture for creating a spark context."""
    return SparkSession.builder. \
        appName("pyspark-test"). \
        getOrCreate()


def test_get_year_month():
    spark = spark_session()
    mock_data: list = [('2013-12-20T00:00:00.000',)]
    expected_result: list = ['2013-12-20 00:00:00', 2013, 12]
    schema: list = ['A']

    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    mock_df=mock_df.withColumn('A',f.to_timestamp(mock_df['A']))
    df_result=get_year_month(df_spark=mock_df, date_col= 'A', year_col='B', month_col='C')
    df_result = df_result.withColumn('A',mock_df['A'].cast(StringType()))
    assert list(df_result.collect()[0][:]) == expected_result
