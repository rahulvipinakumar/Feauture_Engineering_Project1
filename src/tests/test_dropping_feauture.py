from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType,StringType
from utils.dropping_feauture import drop_feauture

def spark_session():
    """Fixture for creating a spark context."""
    return SparkSession.builder. \
        appName("pyspark-test"). \
        getOrCreate()


def test_dropping_feauture():
    spark = spark_session()
    mock_data: list = [('X', 1, 'Annual'),('X', 3, 'Annual'), ('X', 4, 'Daily'), ('Y', 2,  'Daily')]
    expected_result: int = 1
    schema: list = ['A', 'B']

    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    df_result=drop_feauture(df_spark=mock_df, col_list= ['C'])
    assert df_result.columns == expected_result
