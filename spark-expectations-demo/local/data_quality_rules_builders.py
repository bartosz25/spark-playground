from dataclasses import dataclass
from typing import Optional, List

from pyspark import Row

def product_id_from_table_name(table_name: str) -> str:
    return f'{table_name}_validation'

@dataclass
class DataQualityRulesBuilder:
    table_name: str

    def _create_rule(self, rule_type: str, column: Optional[str], expectation: str, failed_action: str, name: str) -> Row:
        return Row(
            product_id=product_id_from_table_name(self.table_name),
            table_name=f'{self.table_name}',
            #table_name=f'{self.table_name}_staging',
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
        rules_builder.new_row_rule(column='creation_date', failed_action='drop', expectation='creation_date < NOW()', name='date_from_past'),
        rules_builder.new_row_rule(column='email', failed_action='drop', expectation='email LIKE "%@%"', name='real_email'),
        rules_builder.new_agg_rule(expectation='COUNT(id) > 4', failed_action='ignore', name='correct_count'),
        rules_builder.new_agg_rule(expectation='COUNT(DISTINCT(id)) > 3', failed_action='fail', name='correct_distinct_count'),
        rules_builder.new_query_rule(expectation='''(SELECT COUNT(*) FROM accounts_staging a 
        LEFT ANTI JOIN account_types at ON at.name = a.account_type) = 0''', failed_action='fail', name='referential_check'),
        rules_builder.new_query_rule(expectation='''(SELECT
            SUM(rows_number_new) - SUM(rows_number_old) AS rows_difference
            FROM (
              SELECT COUNT(*) AS rows_number_new, 0 AS rows_number_old FROM accounts_staging 
              UNION ALL
              SELECT 0 AS rows_number_new, COUNT(*) AS rows_number_old FROM accounts
            )) >= 0
        ''', failed_action='fail', name='seasonality_check')
    ]
