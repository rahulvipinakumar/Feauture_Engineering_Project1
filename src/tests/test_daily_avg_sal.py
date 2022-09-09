from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType
from utils.daily_avg_sal import get_daily_avg_sal

def spark_session():
    """Fixture for creating a spark context."""
    return SparkSession.builder. \
        appName("pyspark-test"). \
        getOrCreate()


def test_get_avg_sal():
    spark = spark_session()
    mock_data: list = [(2640, 'Annual')]
    expected_result: int = 10
    schema: list = ['A', 'B']

    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    df_result=get_daily_avg_sal(df_spark=mock_df, sal_avg= 'A', sal_freq_col='B')
    assert df_result.collect()[0].__getitem__('daily_salary_avg') == expected_result
