from datetime import datetime, date, timedelta
from unittest import TestCase

from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor, TimeDeltaSensor, ExternalTaskSensor

from nose.tools import assert_equal

from karajan.conductor import Conductor
from karajan.dependencies import TrackingDependency, DeltaDependency, NothingDependency, TaskDependency
from karajan.engines import ExasolEngine
from tests.helpers.config import ConfigHelper


class TestExasolEngine(TestCase):
    def setUp(self):
        self.engine = ExasolEngine()
        self.conf = ConfigHelper()

    def build_dags(self):
        self.dags = Conductor(self.conf).build(engine=self.engine)
        return self

    def get_operator(self, task_id, dag_id='test_table'):
        try:
            return self.dags[dag_id].get_task(task_id)
        except AirflowException as e:
            print('%s found in %s' % (self.dags[dag_id].task_ids, dag_id))
            raise e

    def test_aggregation_operator_without_parameterization(self):
        op = self.build_dags().get_operator('aggregate_test_aggregation')
        expected = "CREATE TABLE test_schema_tmp.test_table_agg_test_aggregation_{{ ds_nodash }} AS\nSELECT\nkey_column, test_src_column FROM (SELECT * FROM DUAL) sub "
        assert_str_equal(expected, op.sql)

    def test_aggregation_operator_with_parameterized_context(self):
        self.conf.parameterize_context()
        op = self.build_dags().get_operator('aggregate_test_aggregation')
        expected = "CREATE TABLE test_schema_tmp.test_table_agg_test_aggregation_{{ ds_nodash }} AS\nSELECT\nkey_column, item_column, test_src_column FROM (SELECT * FROM DUAL) sub WHERE item_column in ('g9', 'g9i')"
        assert_str_equal(expected, op.sql)

    def test_aggregation_operator_with_parameterized_context_and_aggregation(self):
        self.conf.parameterize_context().parameterize_aggregation()
        op = self.build_dags().get_operator('aggregate_test_aggregation_g9i')
        expected = "CREATE TABLE test_schema_tmp.test_table_agg_test_aggregation_g9i_{{ ds_nodash }} AS\nSELECT\nkey_column, 'g9i' as item_column, test_src_column FROM (SELECT * FROM g9i) sub "
        assert_str_equal(expected, op.sql)

    def test_param_column_operator_with_item(self):
        self.conf.parameterize_context().with_parameter_columns()
        op = self.build_dags().get_operator('merge_parameter_columns_g9')
        expected = [
            "UPDATE test_schema.test_table SET datetime_col = '2017-01-01 00:00:00' WHERE (datetime_col IS NULL OR datetime_col != '2017-01-01 00:00:00') AND item_column = 'g9'",
            "UPDATE test_schema.test_table SET number_col = 42 WHERE (number_col IS NULL OR number_col != 42) AND item_column = 'g9'",
            "UPDATE test_schema.test_table SET bool_col = True WHERE (bool_col IS NULL OR bool_col != True) AND item_column = 'g9'",
            "UPDATE test_schema.test_table SET date_col = '2017-01-01' WHERE (date_col IS NULL OR date_col != '2017-01-01') AND item_column = 'g9'",
        ]
        assert_equal(expected, op.sql)
        assert_equal(True, op.depends_on_past)

    def test_param_column_operator_without_item(self):
        self.conf.with_parameter_columns()
        op = self.build_dags().get_operator('merge_parameter_columns')
        expected = [
            "UPDATE test_schema.test_table SET datetime_col = '2017-01-01 00:00:00' WHERE (datetime_col IS NULL OR datetime_col != '2017-01-01 00:00:00')",
            "UPDATE test_schema.test_table SET number_col = 42 WHERE (number_col IS NULL OR number_col != 42)",
            "UPDATE test_schema.test_table SET bool_col = True WHERE (bool_col IS NULL OR bool_col != True)",
            "UPDATE test_schema.test_table SET date_col = '2017-01-01' WHERE (date_col IS NULL OR date_col != '2017-01-01')",
        ]
        assert_equal(expected, op.sql)
        assert_equal(True, op.depends_on_past)

    def test_delta_dep_op(self):
        dep = DeltaDependency({
            'delta': '2h',
        })
        task_id = 'wait_for_tracking_schema_tracking_table'
        op = self.engine.dependency_operator(task_id, None, dep)
        expected = timedelta(hours=2)
        assert_equal(expected, op.delta)
        assert_equal(True, isinstance(op, TimeDeltaSensor))

    def test_tracking_dep_op(self):
        dep = TrackingDependency({
            'schema': 'tracking_schema',
            'table': 'tracking_table',
        })
        task_id = 'wait_for_tracking_schema_tracking_table'
        op = self.engine.dependency_operator(task_id, None, dep)
        expected = "SELECT created_date FROM tracking_schema.tracking_table WHERE CREATED_DATE>'{{ ds }}' LIMIT 1"
        assert_equal(expected, op.sql)
        assert_equal(True, isinstance(op, SqlSensor))

    def test_nothing_dep_op(self):
        dep = NothingDependency()
        task_id = 'wait_for_nothing'
        op = self.engine.dependency_operator(task_id, None, dep)
        assert_equal(True, isinstance(op, DummyOperator))

    def test_task_dep_op(self):
        dep = TaskDependency({
            'dag_id': 'dag_id',
            'task_id': 'task_id',
        })
        task_id = 'wait_for_dag_id_task_id'
        op = self.engine.dependency_operator(task_id, None, dep)
        assert_equal('dag_id', op.external_dag_id)
        assert_equal('task_id', op.external_task_id)
        assert_equal(True, isinstance(op, ExternalTaskSensor))

    def test_unknown_dep_op(self):
        exception = None
        try:
            op = self.engine.dependency_operator('', None, None)
        except StandardError as e:
            exception = e
        assert_equal(True, isinstance(exception, StandardError))

    def test_merge_operator_without_parameterization(self):
        op = self.build_dags().get_operator('merge_test_aggregation')
        expected = """
MERGE INTO test_schema.test_table tbl
USING (SELECT key_column, test_src_column FROM test_schema_tmp.test_table_agg_test_aggregation_{{ ds_nodash }}) tmp
ON tbl.key_column=tmp.key_column
WHEN MATCHED THEN
UPDATE SET
tbl.test_column = IFNULL(tmp.test_src_column, tbl.test_column)
WHEN NOT MATCHED THEN
INSERT (key_column, test_column)
VALUES (tmp.key_column, tmp.test_src_column)
        """
        assert_str_equal(expected, op.sql)

    def test_merge_operator_with_parameterized_context(self):
        self.conf.parameterize_context()
        op = self.build_dags().get_operator('merge_test_aggregation')
        expected = """
MERGE INTO test_schema.test_table tbl
USING (SELECT key_column, item_column, test_src_column FROM test_schema_tmp.test_table_agg_test_aggregation_{{ ds_nodash }}) tmp
ON tbl.key_column=tmp.key_column AND tbl.item_column=tmp.item_column
WHEN MATCHED THEN
UPDATE SET
tbl.test_column = IFNULL(tmp.test_src_column, tbl.test_column)
WHEN NOT MATCHED THEN
INSERT (key_column, item_column, test_column)
VALUES (tmp.key_column, tmp.item_column, tmp.test_src_column)
        """
        assert_equal(True, op.depends_on_past)
        assert_str_equal(expected, op.sql)

    def test_merge_operator_with_parameterized_contextand_aggregation(self):
        self.conf.parameterize_context().parameterize_aggregation()
        op = self.build_dags().get_operator('merge_test_aggregation_g9i')
        expected = """
MERGE INTO test_schema.test_table tbl
USING (SELECT key_column, item_column, test_src_column FROM test_schema_tmp.test_table_agg_test_aggregation_g9i_{{ ds_nodash }}) tmp
ON tbl.key_column=tmp.key_column AND tbl.item_column=tmp.item_column
WHEN MATCHED THEN
UPDATE SET
tbl.test_column = IFNULL(tmp.test_src_column, tbl.test_column)
WHEN NOT MATCHED THEN
INSERT (key_column, item_column, test_column)
VALUES (tmp.key_column, tmp.item_column, tmp.test_src_column)
        """
        assert_equal(True, op.depends_on_past)
        assert_str_equal(expected, op.sql)

    def test_cleanup_operator_without_parameterization(self):
        op = self.build_dags().get_operator('cleanup_test_aggregation')
        expected = "DROP TABLE IF EXISTS test_schema_tmp.test_table_agg_test_aggregation_{{ ds_nodash }}"
        assert_str_equal(expected, op.sql)

    def test_cleanup_operator_with_parameterized_context(self):
        self.conf.parameterize_context()
        op = self.build_dags().get_operator('cleanup_test_aggregation')
        expected = "DROP TABLE IF EXISTS test_schema_tmp.test_table_agg_test_aggregation_{{ ds_nodash }}"
        assert_str_equal(expected, op.sql)

    def test_cleanup_operator_with_parameterized_context_and_aggregation(self):
        self.conf.parameterize_context().parameterize_aggregation()
        op = self.build_dags().get_operator('cleanup_test_aggregation_g9i')
        expected = "DROP TABLE IF EXISTS test_schema_tmp.test_table_agg_test_aggregation_g9i_{{ ds_nodash }}"
        assert_str_equal(expected, op.sql)

    def test_init_operator_without_timeseries(self):
        op = self.build_dags().get_operator('init')
        assert_equal(isinstance(op, DummyOperator), True)

    def test_init_op_w_timeseries(self):
        self.conf.with_timeseries()
        op = self.build_dags().get_operator('init')
        expected = "DELETE FROM test_schema.test_table WHERE timeseries_column = '{{ ds }}'"
        assert_str_equal(expected, op.sql)


# TODO sql equal check
def assert_str_equal(actual, expected, strip=True):
    actual = actual.split('\n')
    expected = expected.split('\n')
    for al, el in zip(actual, expected):
        if strip:
            al = al.strip()
            el = el.strip()
        assert_equal(al, el)
