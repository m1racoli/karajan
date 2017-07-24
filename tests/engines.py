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
        self.engine = ExasolEngine('tmp_schema')
        self.conf = ConfigHelper()
        self.dags = {}

    def build_dags(self):
        Conductor(self.conf).build('test_dag', engine=self.engine, output=self.dags)
        return self

    def get_operator(self, task_id, subdag=None, dag_id='test_dag'):
        dag = self.dags[dag_id].get_task(subdag).subdag if subdag else self.dags[dag_id]
        try:
            return dag.get_task(task_id)
        except AirflowException as e:
            print('%s found in %s' % (dag.task_ids, dag))
            raise e

    def test_aggregation_operator_without_parameterization(self):
        op = self.build_dags().get_operator('aggregate_test_aggregation')
        expected = "CREATE TABLE tmp_schema.test_dag_agg_test_aggregation_{{ ds_nodash }} AS\nSELECT\nanother_table_test_src_column, test_src_column, key_column, another_test_src_column FROM (SELECT * FROM DUAL) sub "
        assert_str_equal(expected, op.sql)

    def test_aggregation_operator_with_timeseries(self):
        self.conf.with_timeseries()
        op = self.build_dags().get_operator('aggregate_test_aggregation')
        expected = "CREATE TABLE tmp_schema.test_dag_agg_test_aggregation_{{ ds_nodash }} AS\nSELECT\ntimeseries_column, another_table_test_src_column, test_src_column, key_column, another_test_src_column FROM (SELECT * FROM DUAL) sub "
        assert_str_equal(expected, op.sql)

    def test_aggregation_operator_with_parameterized_context(self):
        self.conf.parameterize_context()
        op = self.build_dags().get_operator('aggregate_test_aggregation', subdag='item')
        expected = "CREATE TABLE tmp_schema.test_dag_item_agg_test_aggregation_{{ ds_nodash }} AS\nSELECT\nanother_table_test_src_column, test_src_column, key_column, item_column, another_test_src_column FROM (SELECT * FROM DUAL) sub WHERE item_column = 'item'"
        assert_str_equal(expected, op.sql)

    def test_aggregation_operator_with_parameterized_context_and_aggregation(self):
        self.conf.parameterize_context().parameterize_aggregation()
        op = self.build_dags().get_operator('aggregate_test_aggregation', subdag='item')
        expected = "CREATE TABLE tmp_schema.test_dag_item_agg_test_aggregation_{{ ds_nodash }} AS\nSELECT\nanother_table_test_src_column, test_src_column, key_column, 'item' as item_column, another_test_src_column FROM (SELECT * FROM item) sub "
        assert_str_equal(expected, op.sql)

    def test_aggregation_operator_with_offset(self):
        self.conf.with_offset()
        op = self.build_dags().get_operator('aggregate_test_aggregation')
        expected = "CREATE TABLE tmp_schema.test_dag_agg_test_aggregation_{{ ds_nodash }} AS\nSELECT\nanother_table_test_src_column, test_src_column, key_column, another_test_src_column FROM (SELECT * FROM DUAL WHERE dt = '{{ macros.ds_add(ds, -1) }}') sub"
        assert_str_equal(expected, op.sql)

    def test_param_column_operator_with_item(self):
        self.conf.parameterize_context().with_parameter_columns()
        op = self.build_dags().get_operator('fill_parameter_columns_test_table', 'item')
        expected = [
            "UPDATE test_schema.test_table SET datetime_col = '2017-01-01 00:00:00' WHERE (datetime_col IS NULL OR datetime_col != '2017-01-01 00:00:00') AND item_column = 'item'",
            "UPDATE test_schema.test_table SET number_col = 42 WHERE (number_col IS NULL OR number_col != 42) AND item_column = 'item'",
            "UPDATE test_schema.test_table SET bool_col = True WHERE (bool_col IS NULL OR bool_col != True) AND item_column = 'item'",
            "UPDATE test_schema.test_table SET date_col = '2017-01-01' WHERE (date_col IS NULL OR date_col != '2017-01-01') AND item_column = 'item'",
        ]
        assert_equal(expected, op.sql)
        assert_equal(True, op.depends_on_past)

    def test_param_column_operator_without_item(self):
        self.conf.with_parameter_columns()
        op = self.build_dags().get_operator('fill_parameter_columns_test_table')
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
        op = self.build_dags().get_operator('merge_test_aggregation_test_table')
        expected = """
MERGE INTO test_schema.test_table tbl
USING (SELECT key_column, test_src_column, another_test_src_column FROM tmp_schema.test_dag_agg_test_aggregation_{{ ds_nodash }}) tmp
ON tbl.key_column=tmp.key_column
WHEN MATCHED THEN
UPDATE SET
tbl.test_column = IFNULL(tmp.test_src_column, tbl.test_column),
tbl.another_test_column = COALESCE(GREATEST(tbl.another_test_column, tmp.another_test_src_column), tbl.another_test_column, tmp.another_test_src_column)
WHEN NOT MATCHED THEN
INSERT (key_column, test_column, another_test_column)
VALUES (tmp.key_column, tmp.test_src_column, tmp.another_test_src_column)
        """
        assert_str_equal(expected, op.sql)

    def test_merge_operator_with_parameterized_context(self):
        self.conf.parameterize_context()
        op = self.build_dags().get_operator('merge_test_aggregation_test_table', 'item')
        expected = """
MERGE INTO test_schema.test_table tbl
USING (SELECT key_column, item_column, test_src_column, another_test_src_column FROM tmp_schema.test_dag_item_agg_test_aggregation_{{ ds_nodash }}) tmp
ON tbl.key_column=tmp.key_column AND tbl.item_column=tmp.item_column
WHEN MATCHED THEN
UPDATE SET
tbl.test_column = IFNULL(tmp.test_src_column, tbl.test_column),
tbl.another_test_column = COALESCE(GREATEST(tbl.another_test_column, tmp.another_test_src_column), tbl.another_test_column, tmp.another_test_src_column)
WHEN NOT MATCHED THEN
INSERT (key_column, item_column, test_column, another_test_column)
VALUES (tmp.key_column, tmp.item_column, tmp.test_src_column, tmp.another_test_src_column)
        """
        assert_equal(True, op.depends_on_past)
        assert_str_equal(expected, op.sql)

    def test_merge_operator_with_parameterized_contextand_aggregation(self):
        self.conf.parameterize_context().parameterize_aggregation()
        op = self.build_dags().get_operator('merge_test_aggregation_test_table', 'item')
        expected = """
MERGE INTO test_schema.test_table tbl
USING (SELECT key_column, item_column, test_src_column, another_test_src_column FROM tmp_schema.test_dag_item_agg_test_aggregation_{{ ds_nodash }}) tmp
ON tbl.key_column=tmp.key_column AND tbl.item_column=tmp.item_column
WHEN MATCHED THEN
UPDATE SET
tbl.test_column = IFNULL(tmp.test_src_column, tbl.test_column),
tbl.another_test_column = COALESCE(GREATEST(tbl.another_test_column, tmp.another_test_src_column), tbl.another_test_column, tmp.another_test_src_column)
WHEN NOT MATCHED THEN
INSERT (key_column, item_column, test_column, another_test_column)
VALUES (tmp.key_column, tmp.item_column, tmp.test_src_column, tmp.another_test_src_column)
        """
        assert_equal(True, op.depends_on_past)
        assert_str_equal(expected, op.sql)

    def test_merge_operator_with_timeseries(self):
        self.conf.with_timeseries()
        op = self.build_dags().get_operator('merge_test_aggregation_test_table')
        expected = """
        MERGE INTO test_schema.test_table tbl
        USING (SELECT key_column, timeseries_column, test_src_column, another_test_src_column FROM tmp_schema.test_dag_agg_test_aggregation_{{ ds_nodash }}) tmp
        ON tbl.key_column=tmp.key_column AND tbl.timeseries_column=tmp.timeseries_column
        WHEN MATCHED THEN
        UPDATE SET
        tbl.test_column = IFNULL(tmp.test_src_column, tbl.test_column),
        tbl.another_test_column = COALESCE(GREATEST(tbl.another_test_column, tmp.another_test_src_column), tbl.another_test_column, tmp.another_test_src_column)
        WHEN NOT MATCHED THEN
        INSERT (key_column, timeseries_column, test_column, another_test_column)
        VALUES (tmp.key_column, tmp.timeseries_column, tmp.test_src_column, tmp.another_test_src_column)
                """
        assert_str_equal(expected, op.sql)

    def test_cleanup_operator_without_parameterization(self):
        op = self.build_dags().get_operator('cleanup_test_aggregation')
        expected = "DROP TABLE IF EXISTS tmp_schema.test_dag_agg_test_aggregation_{{ ds_nodash }}"
        assert_str_equal(expected, op.sql)

    def test_cleanup_operator_with_parameterized_context(self):
        self.conf.parameterize_context()
        op = self.build_dags().get_operator('cleanup_test_aggregation', 'item')
        expected = "DROP TABLE IF EXISTS tmp_schema.test_dag_item_agg_test_aggregation_{{ ds_nodash }}"
        assert_str_equal(expected, op.sql)

    def test_cleanup_operator_with_parameterized_context_and_aggregation(self):
        self.conf.parameterize_context().parameterize_aggregation()
        op = self.build_dags().get_operator('cleanup_test_aggregation', 'item')
        expected = "DROP TABLE IF EXISTS tmp_schema.test_dag_item_agg_test_aggregation_{{ ds_nodash }}"
        assert_str_equal(expected, op.sql)

    def test_prepare_operator_without_timeseries(self):
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table')
        assert_equal(isinstance(op, DummyOperator), True)

    def test_prepare_operator_with_timeseries(self):
        self.conf.with_timeseries()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column = '{{ ds }}'"
        assert_str_equal(expected, op.sql)

    def test_prepare_operator_with_timeseries_and_parameterized_context(self):
        self.conf.with_timeseries().parameterize_context()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table', 'item')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column = '{{ ds }}' AND item_column = 'item'"
        assert_str_equal(expected, op.sql)

    def test_prepare_operator_with_timeseries_and_parameterized_context_and_aggregation(self):
        self.conf.with_timeseries().parameterize_context().parameterize_aggregation()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table', 'item')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column = '{{ ds }}' AND item_column = 'item'"
        assert_str_equal(expected, op.sql)

    def test_prepare_operator_with_timeseries_and_offset(self):
        self.conf.with_timeseries().with_offset()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column = '{{ macros.ds_add(ds, -1) }}'"
        assert_str_equal(expected, op.sql)

    def test_purge_operator_without_timeseries(self):
        op = self.build_dags().get_operator('purge_test_table')
        assert_equal(isinstance(op, DummyOperator), True)

    def test_purge_operator_with_timeseries(self):
        self.conf.with_timeseries()
        op = self.build_dags().get_operator('purge_test_table')
        # expected = "DELETE FROM test_schema.test_table WHERE timeseries_column = '{{ ds }}' AND test_column = NULL"
        # assert_str_equal(expected, op.sql)
        assert_equal(isinstance(op, DummyOperator), True)

    def test_purge_operator_with_timeseries_and_parameterized_context(self):
        self.conf.with_timeseries().parameterize_context()
        op = self.build_dags().get_operator('purge_test_table', 'item')
        # expected = "DELETE FROM test_schema.test_table WHERE timeseries_column = '{{ ds }}' AND item_column = 'item' AND test_column = NULL"
        # assert_str_equal(expected, op.sql)
        assert_equal(isinstance(op, DummyOperator), True)

    def test_purge_operator_with_timeseries_and_offset(self):
        self.conf.with_timeseries().with_offset()
        op = self.build_dags().get_operator('purge_test_table')
        # expected = "DELETE FROM test_schema.test_table WHERE timeseries_column = '{{ macros.ds_add(ds, -1) }}' AND test_column = NULL"
        # assert_str_equal(expected, op.sql)
        assert_equal(isinstance(op, DummyOperator), True)


# TODO sql equal check
def assert_str_equal(actual, expected, strip=True):
    actual = actual.split('\n')
    expected = expected.split('\n')
    for al, el in zip(actual, expected):
        if strip:
            al = al.strip()
            el = el.strip()
        assert_equal(al, el)
