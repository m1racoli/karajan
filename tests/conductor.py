from unittest import TestCase

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

    def get_dag(self, dag_id, item=None):
        dag_id = "%s_%s" % (dag_id, item) if item else dag_id
        self.assertIn(dag_id, self.dags)
        return self.dags[dag_id]

    def get_operator(self, task_id, item=None, dag_id='test_dag'):
        dag = self.get_dag(dag_id, item)
        self.assertIn(task_id, dag.task_dict)
        return dag.get_task(task_id)

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

    def test_merge_operator_bootstrap(self):
        self.conf.parameterize_context().with_timeseries()
        self.engine.describe.return_value = defaults.DESCRIBE_SRC_COLUMNS
        self.build_dags().execute('merge_test_aggregation_test_table', 'item')
        self.engine.describe.assert_called_with(defaults.TMP_ITEM_TABLE_NAME)
        self.engine.bootstrap.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                                 defaults.DESCRIBE_TARGET_COLUMNS)

    def test_merge_operator_delete_existing_data_without_timeseries(self):
        self.build_dags().execute('merge_test_aggregation_test_table')
        self.engine.delete_timeseries.assert_not_called()

    def test_merge_operator_delete_existing_data_with_timeseries(self):
        self.conf.with_timeseries()
        self.build_dags().execute('merge_test_aggregation_test_table')
        self.engine.delete_timeseries.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                                         defaults.TARGET_VALUE_COLUMNS,
                                                         {defaults.TIMESERIES_KEY: defaults.DATE_RANGE})

    def test_merge_operator_delete_existing_data_with_timeseries_parameterization(self):
        self.conf.with_timeseries().parameterize_context()
        self.build_dags().execute('merge_test_aggregation_test_table', 'item')
        self.engine.delete_timeseries.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                                         defaults.TARGET_VALUE_COLUMNS,
                                                         {defaults.TIMESERIES_KEY: defaults.DATE_RANGE,
                                                          'item_column': 'item'})

    def test_merge_operator_delete_existing_data_with_timeseries_offsets_and_reruns(self):
        self.conf.with_timeseries().with_reruns().with_offset()
        self.build_dags().execute('merge_test_aggregation_test_table')
        self.engine.delete_timeseries.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                                         defaults.TARGET_VALUE_COLUMNS,
                                                         {defaults.TIMESERIES_KEY: ('2017-07-28', '2017-07-31')})

    def test_merge_operator_merge(self):
        self.build_dags().execute('merge_test_aggregation_test_table')
        self.engine.merge.assert_called_with(defaults.TMP_TABLE_NAME, defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                             ['key_column'], defaults.MERGE_VALUE_COLUMNS, defaults.MERGE_UPDATE_TYPES)

    def test_merge_operator_merge_with_parametrization(self):
        self.conf.parameterize_context()
        self.build_dags().execute('merge_test_aggregation_test_table', 'item')
        self.engine.merge.assert_called_with(defaults.TMP_ITEM_TABLE_NAME, defaults.TARGET_SCHEMA_NAME,
                                             defaults.TARGET_NAME, ['key_column', 'item_column'],
                                             defaults.MERGE_VALUE_COLUMNS, defaults.MERGE_UPDATE_TYPES)

    def test_merge_operator_merge_with_timeseries(self):
        self.conf.with_timeseries()
        self.build_dags().execute('merge_test_aggregation_test_table')
        self.engine.merge.assert_called_with(defaults.TMP_TABLE_NAME, defaults.TARGET_SCHEMA_NAME,
                                             defaults.TARGET_NAME, ['key_column', 'timeseries_column'],
                                             defaults.MERGE_VALUE_COLUMNS, None)

    def test_merge_operator_merge_with_timeseries_and_parametrization(self):
        self.conf.parameterize_context().with_timeseries()
        self.build_dags().execute('merge_test_aggregation_test_table', 'item')
        self.engine.merge.assert_called_with(defaults.TMP_ITEM_TABLE_NAME, defaults.TARGET_SCHEMA_NAME,
                                             defaults.TARGET_NAME,
                                             ['key_column', 'timeseries_column', 'item_column'],
                                             defaults.MERGE_VALUE_COLUMNS, None)

    def test_finish_operator_purge_without_timeseries(self):
        self.build_dags().execute('finish_test_table')
        self.engine.purge.assert_not_called()

    def test_finish_operator_purge_with_timeseries(self):
        self.conf.with_timeseries()
        self.build_dags().execute('finish_test_table')
        self.engine.purge.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                             defaults.TARGET_ALL_VALUE_COLUMNS,
                                             {defaults.TIMESERIES_KEY: defaults.DATE_RANGE})

    def test_finish_operator_purge_with_timeseries_and_parametetrization(self):
        self.conf.with_timeseries().parameterize_context()
        self.build_dags().execute('finish_test_table', 'item')
        self.engine.purge.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                             defaults.TARGET_ALL_VALUE_COLUMNS,
                                             {defaults.TIMESERIES_KEY: defaults.DATE_RANGE, 'item_column': 'item'})

    def test_finish_operator_purge_with_timeseries_reruns_and_offsets(self):
        self.conf.with_timeseries().with_offset().with_reruns()
        self.build_dags().execute('finish_test_table')
        self.engine.purge.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                             defaults.TARGET_ALL_VALUE_COLUMNS,
                                             {defaults.TIMESERIES_KEY: ('2017-07-28', '2017-08-01')})

    def test_finish_operator_parameters_without_parameter_columns(self):
        self.build_dags().execute('finish_test_table')
        self.engine.parameters.assert_not_called()

    def test_finish_operator_parameters(self):
        self.conf.with_parameter_columns()
        self.build_dags().execute('finish_test_table')
        self.engine.parameters.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                                  defaults.PARAMETER_COLUMNS, None)

    def test_finish_operator_parameters_with_parametrization(self):
        self.conf.with_parameter_columns().parameterize_context()
        self.build_dags().execute('finish_test_table', 'item')
        self.engine.parameters.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                                  defaults.PARAMETER_COLUMNS, {'item_column': 'item'})
