from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType,StringType
from utils.removing_special_char import remove_special_chars

def spark_session():
    """Fixture for creating a spark context."""
    return SparkSession.builder. \
        appName("pyspark-test"). \
        getOrCreate()


def test_remove_special_chars():
    spark = spark_session()
    mock_data: list = [("Ã¢â‚¬Â¢	Excellent interpersonal and organizational skills.",)]
    expected_result: str = "Excellent interpersonal and organizational skills."
    schema: list = ['A']

    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    df_result=remove_special_chars(df_spark=mock_df, col_name= 'A')
    assert df_result.collect()[0].__getitem__('A') == expected_result
