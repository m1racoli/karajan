from unittest import TestCase

from airflow.exceptions import AirflowException
from mock.mock import MagicMock
from nose.tools import assert_equal
from parameterized.parameterized import parameterized

from karajan.conductor import Conductor
from karajan.engines import *
from tests.helpers.assertions import assert_str_equal
from tests.helpers.config import ConfigHelper


class TestBaseEngine(TestCase):
    def setUp(self):
        self.engine = BaseEngine()

    def test_aggregate(self):
        self.assertRaises(NotImplementedError, self.engine.aggregate, 1, 2, 3)

    def test_describe(self):
        self.assertRaises(NotImplementedError, self.engine.describe, 1)

    def test_bootstrap(self):
        self.assertRaises(NotImplementedError, self.engine.bootstrap, 1, 2, 3)

    def test_delete_timeseries(self):
        self.assertRaises(NotImplementedError, self.engine.delete_timeseries, 1, 2, 3, 4)

    def test_merge(self):
        self.assertRaises(NotImplementedError, self.engine.merge, 1, 2, 3, 4, 5)


class TestExasolEngine(TestCase):
    def setUp(self):
        self.engine = ExasolEngine('tmp_schema')
        self.engine._execute = MagicMock()
        self.conf = ConfigHelper()
        self.dags = {}

    def build_dags(self):
        Conductor(self.conf).build('test_dag', engine=self.engine, output=self.dags)
        return self

    def get_operator(self, task_id, post_fix=None, dag_id='test_dag'):
        dag = self.dags["%s_%s" % (dag_id, post_fix)] if post_fix else self.dags[dag_id]
        try:
            return dag.get_task(task_id)
        except AirflowException as e:
            print('%s found in %s' % (dag.task_ids, dag))
            raise e

    def test_aggregate_with_where(self):
        self.engine.aggregate('tmp_table', ['c1', 'c2'], "SELECT nothing", {'item_column': 'item'})
        self.engine._execute.assert_called_with("CREATE TABLE tmp_schema.tmp_table AS SELECT c1, c2 FROM (SELECT nothing) sub WHERE item_column = 'item'")

    def test_aggregate_without_where(self):
        self.engine.aggregate('tmp_table', ['c1', 'c2'], "SELECT nothing", None)
        self.engine._execute.assert_called_with("CREATE TABLE tmp_schema.tmp_table AS SELECT c1, c2 FROM (SELECT nothing) sub ")

    def test_cleanup(self):
        self.engine.cleanup('some_tmp_table')
        self.engine._execute.assert_called_with("DROP TABLE IF EXISTS tmp_schema.some_tmp_table")

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

    def test_prepare_operator_without_timeseries(self):
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table')
        assert_equal(isinstance(op, DummyOperator), True)

    def test_prepare_operator_with_timeseries(self):
        self.conf.with_timeseries()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column BETWEEN '{{ ds }}' AND '{{ ds }}'"
        assert_str_equal(expected, op.sql)

    def test_prepare_operator_with_timeseries_and_parameterized_context(self):
        self.conf.with_timeseries().parameterize_context()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table', 'item')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column BETWEEN '{{ ds }}' AND '{{ ds }}' AND item_column = 'item'"
        assert_str_equal(expected, op.sql)

    def test_prepare_operator_with_timeseries_and_parameterized_context_and_aggregation(self):
        self.conf.with_timeseries().parameterize_context().parameterize_aggregation()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table', 'item')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column BETWEEN '{{ ds }}' AND '{{ ds }}' AND item_column = 'item'"
        assert_str_equal(expected, op.sql)

    def test_prepare_operator_with_timeseries_and_offset(self):
        self.conf.with_timeseries().with_offset()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column BETWEEN '{{ macros.ds_add(ds, -1) }}' AND '{{ macros.ds_add(ds, -1) }}'"
        assert_str_equal(expected, op.sql)

    def test_prepare_operator_with_timeseries_and_reruns(self):
        self.conf.with_timeseries().with_reruns()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column BETWEEN '{{ macros.ds_add(ds, -3) }}' AND '{{ ds }}'"
        assert_str_equal(expected, op.sql)

    def test_prepare_operator_with_timeseries_and_offset_reruns(self):
        self.conf.with_timeseries().with_offset().with_reruns()
        op = self.build_dags().get_operator('prepare_test_aggregation_test_table')
        expected = "UPDATE test_schema.test_table SET test_column = NULL, another_test_column = NULL WHERE timeseries_column BETWEEN '{{ macros.ds_add(ds, -4) }}' AND '{{ macros.ds_add(ds, -1) }}'"
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
