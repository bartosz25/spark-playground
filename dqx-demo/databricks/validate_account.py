from typing import List

from databricks.labs.dqx.row_checks import make_condition
from databricks.labs.dqx.rule import DQColRule
from pyspark.sql import Column, functions as F


def check_email(column_name: str) -> Column:
    column = F.col(column_name)
    return make_condition(~ column.contains("@"), f"Column {column_name} doesn't contain the @", f"{column_name}_valid_email")

def get_simple_rules_for_account() -> List[DQColRule]:
    from databricks.labs.dqx import row_checks
    return [
        DQColRule(col_name='id', name='id is defined', check_func=row_checks.is_not_null, criticality=Criticality.ERROR.value),
        DQColRule(check_func=row_checks.sql_expression,
                  criticality=Criticality.WARN.value,
                  check_func_kwargs={'name': 'creation date is from the past', 'expression':
                      'creation_date < NOW()', 'msg': 'Date is not from the past!'}),
        DQColRule(col_name='email', name='correctly formatted email', check_func=check_email, criticality=Criticality.WARN.value)
    ]

import datetime

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQColRule, Criticality
from databricks.sdk import WorkspaceClient
from pyspark import Row

from pyspark.sql import SparkSession


def create_spark_session_for_localhost_or_databricks() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.master('local[*]').getOrCreate()


def validate_account():
    unity_catalog_schema = 'wfc_catalog.wfc_schema'
    spark_session = create_spark_session_for_localhost_or_databricks()
    spark_session.createDataFrame(data=[
        Row(id=1, name='type a'), Row(id=2, name='type b'), Row(id=3, name='type c')
    ]).write.format('delta').mode('overwrite').saveAsTable(f'{unity_catalog_schema}.account_types')

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
    ],
        schema='id INT, name STRING, email STRING, account_type STRING, creation_date TIMESTAMP, meta_dq_run_id STRING, meta_dq_run_datetime TIMESTAMP')
    accounts_dataframe.write.mode('overwrite').format('delta').saveAsTable(f'{unity_catalog_schema}.accounts')

    validation_rules = get_simple_rules_for_account()
    """        DQColRule(name='more than 4 rows', check_func=row_checks.sql_expression,
                  criticality=Criticality.WARN.value,
                  check_func_kwargs={'expression': 'COUNT(id) > 4', 'msg': 'There are less than 4 rows'}),
        DQColRule(name='more than 3 unique rows', check_func=row_checks.sql_expression,
                  criticality=Criticality.WARN.value,
                  check_func_kwargs={'expression': 'COUNT(DISTINCT(id)) > 3', 'msg': 'There are less than 3 unique rows'}),"""
    account_to_write = spark_session.createDataFrame(data=[
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

    # profile input data
    workspace_client = WorkspaceClient()
    dqx_engine = DQEngine(workspace_client)
    valid_accounts, invalid_accounts = dqx_engine.apply_checks_and_split(account_to_write, validation_rules)
    invalid_accounts.write.mode('overwrite').format('delta').saveAsTable(f'{unity_catalog_schema}.invalid_accounts')

    valid_accounts.createOrReplaceTempView('valid_accounts')

    # Now, we need to tweak the validation rules a bit and apply
    from databricks.labs.dqx import row_checks
    rule_for_types = DQColRule(name='all account types are defined',
                               check_func=row_checks.sql_expression,
                               criticality=Criticality.ERROR.value,
                               check_func_kwargs={'expression': 'missing_types = 0',
                                                  'msg': 'There should not be missing types'})
    missing_types_dataset = spark_session.sql(f'''SELECT COUNT(*) AS missing_types FROM valid_accounts a 
            LEFT ANTI JOIN {unity_catalog_schema}.account_types at ON at.name = a.account_type''')
    _, invalid_types = dqx_engine.apply_checks_and_split(missing_types_dataset, [rule_for_types])

    rule_for_seasonality = DQColRule(name='seasonality check',
                                     check_func=row_checks.sql_expression,
                                     criticality=Criticality.ERROR.value,
                                     check_func_kwargs={'expression': 'rows_difference >= 0',
                                                        'msg': 'There should be more than 0'})
    seasonality_result = spark_session.sql(f'''SELECT
        SUM(rows_number_new) - SUM(rows_number_old) AS rows_difference
        FROM (
          SELECT COUNT(*) AS rows_number_new, 0 AS rows_number_old FROM valid_accounts 
          UNION ALL
          SELECT 0 AS rows_number_new, COUNT(*) AS rows_number_old FROM {unity_catalog_schema}.accounts
        )''')
    _, invalid_seasonality = dqx_engine.apply_checks_and_split(seasonality_result, [rule_for_seasonality])

    if not invalid_seasonality.isEmpty() or not missing_types_dataset.isEmpty():
        raise RuntimeError('Detected some validation issues for seasonality or data integrity: '
                           f'seasonality result: {invalid_seasonality.collect()}'
                           f'types result: {missing_types_dataset.collect()}')
