from pyspark.sql import SparkSession
from utils.gap_filling import gap_filling_by_mode


def spark_session():
    """Fixture for creating a spark context."""
    return SparkSession.builder. \
        appName("pyspark-test"). \
        getOrCreate()


def test_gap_filling_by_mode():
    spark = spark_session()
    mock_data: list = [('X', 1, 'Annual'),('X', 3, 'Annual'), ('X', 4, 'Daily'), ('X', 2, None), ('Y', 2,  'Daily')]
    expected_result: str = 'Annual'
    schema: list = ['A', 'B', 'C']

    mock_df = spark.createDataFrame(data=mock_data, schema=schema)
    df_result=gap_filling_by_mode(df_spark=mock_df, grp_by_cols= ['A'], req_col='C')
    assert df_result.filter((df_result['A']=='X') & (df_result['b']==2)).collect()[0].__getitem__('C') == expected_result
