from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType,StringType
from utils.encoding import one_hot_encoding

def spark_session():
    """Fixture for creating a spark context."""
    return SparkSession.builder. \
        appName("pyspark-test"). \
        getOrCreate()


def test_encoding():
    spark = spark_session()
    mock_data: list = [('X', 1, 'Annual'),('X', 3, 'Annual'), ('X', 4, 'Daily'), ('Y', 2,  'Daily')]
    expected_result: int = 1
    schema: list = ['A', 'B', 'C']

    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    df_result=one_hot_encoding(df_spark=mock_df, col_name= 'C')
    assert df_result.collect()[0].__getitem__('Annual') == expected_result
