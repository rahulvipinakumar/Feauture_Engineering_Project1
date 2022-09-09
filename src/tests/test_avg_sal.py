from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType
from utils.avg_sal import get_avg_sal

def spark_session():
    """Fixture for creating a spark context."""
    return SparkSession.builder. \
        appName("pyspark-test"). \
        getOrCreate()


def test_get_avg_sal():
    spark = spark_session()
    mock_data: list = [(10, 20)]
    expected_result: int = 15
    schema: list = ['A', 'B']

    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    df_result=get_avg_sal(df_spark=mock_df, sal_from= 'A', sal_to='B')
    assert df_result.collect()[0].__getitem__('salary_avg') == expected_result
