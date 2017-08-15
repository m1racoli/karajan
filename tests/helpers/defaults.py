from datetime import datetime

EXECUTION_DATE = datetime(2017, 8, 1)
DATE_RANGE = ('2017-08-01', '2017-08-01')
TMP_TABLE_NAME = 'test_dag_agg_test_aggregation_20170801'
TMP_ITEM_TABLE_NAME = 'test_dag_item_agg_test_aggregation_20170801'
TARGET_NAME = 'test_table'
TARGET_SCHEMA_NAME = 'test_schema'
TIMESERIES_KEY = 'timeseries_column'
SRC_COLUMNS = ['another_table_test_src_column', 'test_src_column', 'key_column', 'another_test_src_column',
               'item_column', 'timeseries_column']
TARGET_VALUE_COLUMNS = ['test_column', 'another_test_column']
MERGE_VALUE_COLUMNS = {'test_column': 'test_src_column', 'another_test_column': 'another_test_src_column'}
MERGE_UPDATE_TYPES = {'test_column': 'REPLACE', 'another_test_column': 'MAX'}
DESCRIBE_SRC_COLUMNS = {c: 'SOMETYPE' for c in SRC_COLUMNS}
DESCRIBE_TARGET_COLUMNS = {'test_column': 'SOMETYPE', 'key_column': 'SOMETYPE', 'another_test_column': 'SOMETYPE',
                           'item_column': 'SOMETYPE', 'timeseries_column': 'SOMETYPE'}
