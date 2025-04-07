import datetime
from dataclasses import dataclass
from typing import Optional, List


from pyspark import Row
from pyspark.sql import SparkSession

def create_spark_session_for_localhost_or_databricks() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.master('local[*]').getOrCreate()


def product_id_from_table_name(table_name: str) -> str:
   return f'{table_name}_validation'


@dataclass
class DataQualityRulesBuilder:
   table_name: str


   def _create_rule(self, rule_type: str, column: Optional[str], expectation: str, failed_action: str, name: str) -> Row:
       return Row(
           product_id=product_id_from_table_name(self.table_name),
           table_name='wfc.test_se.accounts',
           rule_type=rule_type,
           rule=name,
           column_name=column,
           expectation=expectation,
           action_if_failed=failed_action,
           enable_for_source_dq_validation=True,
           enable_for_target_dq_validation=True,
           is_active=True,
           tag='validity',
           description='...',
           enable_error_drop_alert=True, error_drop_threshold=1
       )


   def new_row_rule(self, column: str, expectation: str, failed_action: str, name: str) -> Row:
       return self._create_rule(
           rule_type='row_dq', column=column, expectation=expectation, failed_action=failed_action, name=name
       )


   def new_agg_rule(self, expectation: str, failed_action: str, name: str) -> Row:
       return self._create_rule(
           rule_type='agg_dq', column=None, expectation=expectation, failed_action=failed_action, name=name
       )


   def new_query_rule(self, expectation: str, failed_action: str, name: str) -> Row:
       return self._create_rule(
           rule_type='query_dq', column=None, expectation=expectation, failed_action=failed_action, name=name
       )




def get_rules_for_account() -> List[Row]:
   rules_builder = DataQualityRulesBuilder('accounts')


   return [
       rules_builder.new_row_rule(column='id', failed_action='fail', expectation='id IS NOT NULL', name='id_defined'),
       rules_builder.new_row_rule(column='creation_date', failed_action='fail', expectation='creation_date < NOW()', name='date_from_past'),
       rules_builder.new_row_rule(column='email', failed_action='fail', expectation='email LIKE "%@%"', name='real_email'),
       rules_builder.new_agg_rule(expectation='COUNT(id) > 4', failed_action='fail', name='correct_count'),
       rules_builder.new_agg_rule(expectation='COUNT(DISTINCT(id)) > 3', failed_action='fail', name='correct_distinct_count'),
       rules_builder.new_query_rule(expectation='''(SELECT COUNT(*) FROM accounts_staging a
       LEFT ANTI JOIN account_types at ON at.name = a.account_type) = 0''', failed_action='fail', name='referential_check'),
       rules_builder.new_query_rule(expectation='''(SELECT
           SUM(rows_number_new) - SUM(rows_number_old) AS rows_difference
           FROM (
             SELECT COUNT(*) AS rows_number_new, 0 AS rows_number_old FROM accounts_staging
             UNION ALL
             SELECT 0 AS rows_number_new, COUNT(*) AS rows_number_old FROM accounts_old
           )) >= 0
       ''', failed_action='fail', name='seasonality_check')
   ]


def load_accounts_if_valid():
   spark_session: SparkSession = create_spark_session_for_localhost_or_databricks()
   from spark_expectations.core.expectations import (
       SparkExpectations,
       WrappedDataFrameWriter,
   )
   from spark_expectations.config.user_config import Constants as user_config
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
       stats_table='wfc.test_se.data_validation_stats',
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
       target_table='wfc.test_se.accounts',
       write_to_table=True,
       user_conf=user_conf,
       target_table_view='accounts_staging'
   )
   def build_account_table():
       spark_session.createDataFrame(data=[
           Row(id=1, name='type a'), Row(id=2, name='type b'), Row(id=3, name='type c')
       ]).createOrReplaceTempView('account_types')
       spark_session.createDataFrame(data=[
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
       ],
           schema='id INT, name STRING, email STRING, account_type STRING, creation_date TIMESTAMP, meta_dq_run_id STRING, meta_dq_run_datetime TIMESTAMP').createOrReplaceTempView('accounts_old')
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


   return build_account_table()
