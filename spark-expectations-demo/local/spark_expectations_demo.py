import datetime

from delta import configure_spark_with_delta_pip
from pyspark import Row
from pyspark.sql import SparkSession
from spark_expectations.core.expectations import (
    SparkExpectations,
    WrappedDataFrameWriter,
)
from spark_expectations.config.user_config import Constants as user_config
from data_quality_rules_builders import get_rules_for_account, product_id_from_table_name

spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                                                .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                                                ).getOrCreate())

### Setup the table
spark_session.createDataFrame(data=[
    Row(id=1, name='type a'), Row(id=2, name='type b'), Row(id=3, name='type c')
]).write.format('delta').mode('overwrite').saveAsTable('account_types')

accounts_dataframe = spark_session.createDataFrame(data=[
    Row(id=1, name='account 1', email='email1@email', account_type='type a',
        creation_date=datetime.datetime(2025, 1, 10, 14, 50), meta_dq_run_id=None, meta_dq_run_datetime=None),
    Row(id=1, name='account 1', email='email1@email', account_type='type a',
        creation_date=datetime.datetime(2025, 1, 10, 14, 50), meta_dq_run_id=None, meta_dq_run_datetime=None),
    Row(id=2, name='account 2', email='email2@email', account_type='type b',
        creation_date=datetime.datetime(2025, 1, 10, 11, 50), meta_dq_run_id=None, meta_dq_run_datetime=None),
    Row(id=3, name='account 3', email='email3@email', account_type='type c',
        creation_date=datetime.datetime(2025, 1, 10, 8, 50), meta_dq_run_id=None, meta_dq_run_datetime=None),
    Row(id=4, name='account 4', email='email4@email', account_type='type XXXX',
        creation_date=datetime.datetime(2025, 1, 9, 14, 50), meta_dq_run_id=None, meta_dq_run_datetime=None)
], schema='id INT, name STRING, email STRING, account_type STRING, creation_date TIMESTAMP, meta_dq_run_id STRING, meta_dq_run_datetime TIMESTAMP')

accounts_dataframe.write.mode('overwrite').format('delta').saveAsTable('accounts')

### Spark Expectations part
# if you don't define the tag, the rules won't be picked up
# if you don't define all the columns, the rules retrieval will fail!
rules_dataframe = spark_session.createDataFrame(data=get_rules_for_account(), schema='''
    product_id STRING,
    table_name STRING,
    rule_type STRING,
    rule STRING,
    column_name STRING,
    expectation STRING,
    action_if_failed STRING,
    enable_for_source_dq_validation BOOLEAN, 
    enable_for_target_dq_validation BOOLEAN,
    is_active BOOLEAN, tag STRING, description STRING, enable_error_drop_alert BOOLEAN, error_drop_threshold INT
''')
delta_writer = WrappedDataFrameWriter().mode("overwrite").format("delta")
spark_expectations = SparkExpectations(
    product_id=product_id_from_table_name('accounts'),
    rules_df=rules_dataframe,
    stats_table='data_validation_stats',
    stats_table_writer=delta_writer,
    target_and_error_table_writer=delta_writer,
    debugger=True,
    stats_streaming_options={user_config.se_enable_streaming: False}
)
user_conf = {
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_enable_slack: False,
}


@spark_expectations.with_expectations(
    target_table='accounts',
    write_to_table=True,
    user_conf=user_conf,
    target_table_view='accounts_staging'
)
def build_account_table():
    df = spark_session.createDataFrame(data=[
        Row(id=1, name='account 1', email='email1@email', account_type='type a',
            creation_date=datetime.datetime(2025, 1, 10, 14, 50)),
        Row(id=1, name='account 1', email='email1@email', account_type='type a',
            creation_date=datetime.datetime(2025, 1, 10, 14, 50)),
        Row(id=2, name='account 5', email='email2@email', account_type='type b',
            creation_date=datetime.datetime(2025, 1, 10, 11, 50)),
        Row(id=3, name='account 6', email='email3@email', account_type='type c',
            creation_date=datetime.datetime(2025, 1, 10, 8, 50)),
        Row(id=4, name='account 7', email='email3@email', account_type='type c',
            creation_date=datetime.datetime(2050, 1, 10, 8, 50)),
        Row(id=None, name='account None', email='email3@email', account_type='type c',
            creation_date=datetime.datetime(2025, 1, 10, 8, 50)),
        Row(id=111, name='account 111', email='email3@email', account_type='type XXXXXXXc',
            creation_date=datetime.datetime(2025, 1, 10, 8, 50)),
    ], schema='id INT, name STRING, email STRING, account_type STRING, creation_date TIMESTAMP')

    df.createOrReplaceTempView('accounts_staging')

    return df

build_account_table()

spark_session.sql('SELECT * FROM data_validation_stats').show(truncate=False)
spark_session.sql('SELECT * FROM accounts').show(truncate=False)