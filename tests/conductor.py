from unittest import TestCase

from airflow.exceptions import AirflowException
from mock import MagicMock

from karajan.conductor import Conductor
from tests.helpers import defaults
from tests.helpers.config import ConfigHelper


class TestConductor(TestCase):
    def setUp(self):
        self.engine = MagicMock()
        self.conf = ConfigHelper()
        self.dags = {}

    def build_dags(self):
        Conductor(self.conf).build('test_dag', engine=self.engine, output=self.dags)
        return self

    def context(self, dag_id, item=None, ds=defaults.EXECUTION_DATE):
        return {
            'ds': ds.strftime("%Y-%m-%d"),
            'ds_nodash': ds.strftime("%Y%m%d"),
            'dag': self.dags["%s_%s" % (dag_id, item) if item else dag_id]
        }

    def get_operator(self, task_id, post_fix=None, dag_id='test_dag'):
        dag = self.dags["%s_%s" % (dag_id, post_fix)] if post_fix else self.dags[dag_id]
        try:
            return dag.get_task(task_id)
        except AirflowException as e:
            print('%s found in %s' % (dag.task_ids, dag))
            raise e

    def execute(self, task_id, item=None, dag_id='test_dag'):
        op = self.get_operator(task_id, item)
        op.execute(self.context(dag_id, item))
        return self

    def test_cleanup_operator(self):
        self.build_dags().execute('cleanup_test_aggregation')
        self.engine.cleanup.assert_called_with(
            defaults.TMP_TABLE_NAME,
        )

    def test_cleanup_operator_with_params(self):
        self.conf.parameterize_context()
        self.build_dags().execute('cleanup_test_aggregation', 'item')
        self.engine.cleanup.assert_called_with(
            defaults.TMP_ITEM_TABLE_NAME,
        )

    def test_aggregation_operator_without_parameterization(self):
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            ['another_table_test_src_column', 'test_src_column', 'key_column', 'another_test_src_column'],
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-08-01' AND '2017-08-01'",
            None,
        )

    def test_aggregation_operator_with_timeseries(self):
        self.conf.with_timeseries()
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            ['timeseries_column', 'another_table_test_src_column', 'test_src_column', 'key_column',
             'another_test_src_column'],
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-08-01' AND '2017-08-01'",
            None,
        )

    def test_aggregation_operator_with_other_timeseries(self):
        self.conf.with_timeseries(target_id='another_table')
        self.build_dags().execute('aggregate_another_aggregation')
        self.engine.aggregate.assert_called_with(
            'test_dag_agg_another_aggregation_20170801',
            ['another_aggregation_test_src_column', 'key_column'],
            u"SELECT everything FROM here",
            None,
        )

    def test_aggregation_operator_with_parameterized_context(self):
        self.conf.parameterize_context()
        self.build_dags().execute('aggregate_test_aggregation', 'item')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_ITEM_TABLE_NAME,
            ['another_table_test_src_column', 'test_src_column', 'key_column', 'item_column',
             'another_test_src_column'],
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-08-01' AND '2017-08-01'",
            {'item_column': 'item'},
        )

    def test_aggregation_operator_with_parameterized_context_and_aggregation(self):
        self.conf.parameterize_context().parameterize_aggregation()
        self.build_dags().execute('aggregate_test_aggregation', 'item')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_ITEM_TABLE_NAME,
            ['another_table_test_src_column', 'test_src_column', 'key_column', "'item' as item_column",
             'another_test_src_column'],
            u"SELECT * FROM item WHERE dt BETWEEN '2017-08-01' AND '2017-08-01'",
            None,
        )

    def test_aggregation_operator_with_offset(self):
        self.conf.with_offset()
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            ['another_table_test_src_column', 'test_src_column', 'key_column', 'another_test_src_column'],
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-07-31' AND '2017-07-31'",
            None,
        )

    def test_aggregation_operator_with_reruns(self):
        self.conf.with_reruns()
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            ['another_table_test_src_column', 'test_src_column', 'key_column', 'another_test_src_column'],
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-07-29' AND '2017-08-01'",
            None,
        )

    def test_aggregation_operator_with_offset_and_reruns(self):
        self.conf.with_offset().with_reruns()
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            ['another_table_test_src_column', 'test_src_column', 'key_column', 'another_test_src_column'],
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-07-28' AND '2017-07-31'",
            None,
        )
