from datetime import datetime, date, timedelta
from unittest import TestCase

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import SqlSensor, TimeDeltaSensor, ExternalTaskSensor
from nose.tools import assert_equal
from parameterized import parameterized

from karajan.dependencies import TrackingDependency, DeltaDependency, NothingDependency, TaskDependency
from karajan.engines import ExasolEngine


class TestExasolEngine(TestCase):
    class Stub(object):
        def __init__(self, **kwargs):
            for k,v in kwargs.iteritems():
                setattr(self, k,v)

        def setattr(self, attr, value):
            setattr(self, attr, value)

    def setUp(self):
        self.engine = ExasolEngine()

    def test_agg_op_wo_param(self):
        target = TestExasolEngine.Stub(
            name='test_table',
            schema='test_schema',
            src_column_names = lambda x: ['test_column'],
        )
        agg = TestExasolEngine.Stub(
            name="test_agg",
            query="SELECT * FROM {{ default }}",
        )
        params = {'default': 'DUAL'}
        task_id = 'aggregate_test_agg'
        op = self.engine.aggregation_operator(task_id, None, target, agg, params, None)
        expected = "CREATE TABLE test_schema_tmp.test_table_agg_test_agg_{{ ds_nodash }} AS\nSELECT\ntest_column FROM (SELECT * FROM DUAL) sub "
        assert_equal(expected, op.sql)

    def test_agg_op_w_param_context(self):
        target = TestExasolEngine.Stub(
            name='test_table',
            schema='test_schema',
            src_column_names = lambda x: ['item_column', 'test_column'],
        )
        agg = TestExasolEngine.Stub(
            name="test_agg",
            query="SELECT * FROM {{ default }}",
        )
        params = {'default': 'DUAL'}
        task_id = 'aggregate_test_agg'
        op = self.engine.aggregation_operator(task_id, None, target, agg, params, ('item_column', ['g9i', 'g9']))
        expected = "CREATE TABLE test_schema_tmp.test_table_agg_test_agg_{{ ds_nodash }} AS\nSELECT\nitem_column, test_column FROM (SELECT * FROM DUAL) sub WHERE item_column in ('g9i', 'g9')"
        assert_equal(expected, op.sql)

    def test_agg_op_w_param_context_agg(self):
        target = TestExasolEngine.Stub(
            name='test_table',
            schema='test_schema',
            src_column_names = lambda x: ['item_column', 'test_column'],
        )
        agg = TestExasolEngine.Stub(
            name="test_agg",
            query="SELECT * FROM {{ item }}",
        )
        params = {'item': 'DUAL'}
        task_id = 'aggregate_test_agg_g9i'
        op = self.engine.aggregation_operator(task_id, None, target, agg, params, ('item_column', 'g9i'))
        expected = "CREATE TABLE test_schema_tmp.test_table_agg_test_agg_g9i_{{ ds_nodash }} AS\nSELECT\n'g9i' as item_column, test_column FROM (SELECT * FROM DUAL) sub "
        assert_equal(expected, op.sql)

    def test_param_column_op_w_item(self):
        context = TestExasolEngine.Stub(
            item_column='item_column'
        )
        target = TestExasolEngine.Stub(
            name='test_table',
            schema='test_schema',
            parameter_columns={
                'date_col': 'date',
                'datetime_col': 'datetime',
                'number_col': 'number',
                'bool_col': 'bool',
            },
            context=context
        )
        task_id = 'merge_parameter_columns'
        params = {
            'item': 'g9',
            'date': date(2017, 1, 1),
            'datetime': datetime(2017, 1, 1, 0, 0, 0),
            'number': 42,
            'bool': True,
        }
        op = self.engine.param_column_op(task_id, None, target, params, 'g9')
        expected = [
            "UPDATE test_schema.test_table SET datetime_col = '2017-01-01 00:00:00' WHERE (datetime_col IS NULL OR datetime_col != '2017-01-01 00:00:00') AND item_column = 'g9'",
            "UPDATE test_schema.test_table SET number_col = 42 WHERE (number_col IS NULL OR number_col != 42) AND item_column = 'g9'",
            "UPDATE test_schema.test_table SET bool_col = True WHERE (bool_col IS NULL OR bool_col != True) AND item_column = 'g9'",
            "UPDATE test_schema.test_table SET date_col = '2017-01-01' WHERE (date_col IS NULL OR date_col != '2017-01-01') AND item_column = 'g9'",
        ]
        assert_equal(expected, op.sql)
        assert_equal(True, op.depends_on_past)

    def test_param_column_op_wo_item(self):
        context = TestExasolEngine.Stub(
            item_column='item_column'
        )
        target = TestExasolEngine.Stub(
            name='test_table',
            schema='test_schema',
            parameter_columns={
                'date_col': 'date',
                'datetime_col': 'datetime',
                'number_col': 'number',
                'bool_col': 'bool',
            },
            context=context
        )
        task_id = 'merge_parameter_columns'
        params = {
            'date': date(2017, 1, 1),
            'datetime': datetime(2017, 1, 1, 0, 0, 0),
            'number': 42,
            'bool': True,
        }
        op = self.engine.param_column_op(task_id, None, target, params, '')
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
