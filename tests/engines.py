from unittest import TestCase

from airflow.exceptions import AirflowException
from mock.mock import MagicMock
from nose.tools import assert_equal
from parameterized.parameterized import parameterized

from karajan.conductor import Conductor
from karajan.engines import *
from tests.helpers.config import ConfigHelper


class TestBaseEngine(TestCase):
    def setUp(self):
        self.engine = BaseEngine()

    def test_aggregate(self):
        self.assertRaises(NotImplementedError, self.engine.aggregate, 1, 2, 3)

    def test_cleanup(self):
        self.assertRaises(NotImplementedError, self.engine.cleanup, 1)

    def test_describe(self):
        self.assertRaises(NotImplementedError, self.engine.describe, 1)

    def test_bootstrap(self):
        self.assertRaises(NotImplementedError, self.engine.bootstrap, 1, 2, 3)

    def test_delete_timeseries(self):
        self.assertRaises(NotImplementedError, self.engine.delete_timeseries, 1, 2, 3, 4)

    def test_merge(self):
        self.assertRaises(NotImplementedError, self.engine.merge, 1, 2, 3, 4, 5)

    def test_purge(self):
        self.assertRaises(NotImplementedError, self.engine.purge, 1, 2, 3, 4)

    def test_parameters(self):
        self.assertRaises(NotImplementedError, self.engine.parameters, 1, 2, 3, 4)


class TestExasolEngine(TestCase):
    def setUp(self):
        self.engine = ExasolEngine('tmp_schema')
        self.engine._execute = MagicMock()
        self.engine._select = MagicMock()
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

    def test_describe(self):
        self.engine._select.return_value = [['KEY_COLUMN', 'VARCHAR(20) UTF8'], ['TIMESERIES_COLUMN', 'DATE']]
        src_columns = self.engine.describe('tmp_table')
        self.engine._select.assert_called_with("SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS WHERE COLUMN_TABLE = 'TMP_TABLE' AND COLUMN_SCHEMA = 'TMP_SCHEMA'")
        assert_equal(src_columns, {'key_column': 'VARCHAR(20) UTF8', 'timeseries_column': 'DATE'})

    def test_bootstrap_table_missing(self):
        self.engine._select.return_value = []
        self.engine.bootstrap('some_schema','some_table', {'key_column': 'VARCHAR(20) UTF8', 'timeseries_column': 'DATE'})
        self.engine._select.assert_called_with(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS WHERE COLUMN_TABLE = 'SOME_TABLE' AND COLUMN_SCHEMA = 'SOME_SCHEMA'")
        self.engine._execute.assert_called_with("CREATE TABLE SOME_SCHEMA.SOME_TABLE (TIMESERIES_COLUMN DATE DEFAULT NULL, KEY_COLUMN VARCHAR(20) UTF8 DEFAULT NULL)")

    def test_bootstrap_column_missing(self):
        self.engine._select.return_value = [['KEY_COLUMN', 'VARCHAR(20) UTF8']]
        self.engine.bootstrap('some_schema','some_table', {'key_column': 'VARCHAR(20) UTF8', 'timeseries_column': 'DATE'})
        self.engine._select.assert_called_with(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS WHERE COLUMN_TABLE = 'SOME_TABLE' AND COLUMN_SCHEMA = 'SOME_SCHEMA'")
        self.engine._execute.assert_called_with(["ALTER TABLE SOME_SCHEMA.SOME_TABLE ADD COLUMN TIMESERIES_COLUMN DATE DEFAULT NULL"])

    def test_bootstrap_nothing_missing(self):
        self.engine._select.return_value = [['KEY_COLUMN', 'VARCHAR(20) UTF8'], ['TIMESERIES_COLUMN', 'DATE']]
        self.engine.bootstrap('some_schema','some_table', {'key_column': 'VARCHAR(20) UTF8', 'timeseries_column': 'DATE'})
        self.engine._select.assert_called_with(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS WHERE COLUMN_TABLE = 'SOME_TABLE' AND COLUMN_SCHEMA = 'SOME_SCHEMA'")
        self.engine._execute.assert_not_called()

    def test_delete_timeseries(self):
        self.engine.delete_timeseries('some_schema', 'some_table', ['col_1', 'col_2'], where={'time_col': ('2017-02-28', '2017-04-01'), 'item_col': 'item'})
        self.engine._execute.assert_called_with("UPDATE some_schema.some_table SET col_1 = NULL, col_2 = NULL WHERE time_col BETWEEN '2017-02-28' AND '2017-04-01' AND item_col = 'item'")

    @parameterized.expand([
        ('MIN', "COALESCE(LEAST(tbl.val, tmp.src_val), tbl.val, tmp.src_val)"),
        ('MAX', "COALESCE(GREATEST(tbl.val, tmp.src_val), tbl.val, tmp.src_val)"),
        ('KEEP', "IFNULL(tbl.val, tmp.src_val)"),
        ('REPLACE', "IFNULL(tmp.src_val, tbl.val)"),
    ])
    def test_merge_without_timeseries(self, update_type, stm):
        self.engine.merge('some_tmp_table', 'some_schema', 'some_table', ['key_col'], {'val': 'src_val'}, {'val': update_type})
        self.engine._execute.assert_called_with("""MERGE INTO some_schema.some_table tbl
USING (SELECT key_col, src_val FROM tmp_schema.some_tmp_table) tmp
ON tbl.key_col = tmp.key_col
WHEN MATCHED THEN
UPDATE SET
tbl.val = %s
WHEN NOT MATCHED THEN
INSERT (key_col, val)
VALUES (tmp.key_col, tmp.src_val)""" % stm)

    def test_merge_with_timeseries(self):
        self.engine.merge('some_tmp_table', 'some_schema', 'some_table', ['key_col', 'time_col'], {'val_1': 'src_val_1', 'val_2': 'src_val_2'})
        self.engine._execute.assert_called_with("""MERGE INTO some_schema.some_table tbl
USING (SELECT key_col, time_col, src_val_1, src_val_2 FROM tmp_schema.some_tmp_table) tmp
ON tbl.key_col = tmp.key_col AND tbl.time_col = tmp.time_col
WHEN MATCHED THEN
UPDATE SET
tbl.val_1 = tmp.src_val_1, tbl.val_2 = tmp.src_val_2
WHEN NOT MATCHED THEN
INSERT (key_col, time_col, val_1, val_2)
VALUES (tmp.key_col, tmp.time_col, tmp.src_val_1, tmp.src_val_2)""")

    def test_purge(self):
        self.engine.purge('some_schema', 'some_table', ['col_1', 'col_2'], {'time_col': ('2017-01-31', '2017-02-28'), 'item_col': 'item'})
        self.engine._execute.assert_called_with("DELETE FROM some_schema.some_table WHERE time_col BETWEEN '2017-01-31' AND '2017-02-28' AND item_col = 'item' AND col_1 IS NULL AND col_2 IS NULL")

    def test_parameters(self):
        self.engine.parameters('some_schema', 'some_table', {'dcol': datetime(2017, 1, 1), 'ncol': 23, 'scol': 'str', 'bcol': True}, {'time_col': ('2017-01-31', '2017-02-28'), 'item_col': 'item'})
        self.engine._execute.assert_called_with([
            "UPDATE some_schema.some_table SET bcol = True WHERE time_col BETWEEN '2017-01-31' AND '2017-02-28' AND item_col = 'item' AND (bcol IS NULL OR bcol != True)",
            "UPDATE some_schema.some_table SET scol = 'str' WHERE time_col BETWEEN '2017-01-31' AND '2017-02-28' AND item_col = 'item' AND (scol IS NULL OR scol != 'str')",
            "UPDATE some_schema.some_table SET ncol = 23 WHERE time_col BETWEEN '2017-01-31' AND '2017-02-28' AND item_col = 'item' AND (ncol IS NULL OR ncol != 23)",
            "UPDATE some_schema.some_table SET dcol = '2017-01-01 00:00:00' WHERE time_col BETWEEN '2017-01-31' AND '2017-02-28' AND item_col = 'item' AND (dcol IS NULL OR dcol != '2017-01-01 00:00:00')",
        ])

    # old tests

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
