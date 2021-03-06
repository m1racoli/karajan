#
# Copyright 2017 Wooga GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from datetime import datetime
from unittest import TestCase

from airflow.models import DagRun
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

    def dag_run(self, dag_id, external_trigger=False):
        return DagRun(
            dag_id=dag_id,
            run_id="karajan_run_%s" % datetime.now(),
            external_trigger=external_trigger,
            conf={'start_date': defaults.EXTERNAL_START_DATE, 'end_date': defaults.EXTERNAL_END_DATE} if external_trigger else None,
            execution_date=defaults.EXTERNAL_EXECUTION_DATE if external_trigger else datetime.now(),
            state='running'
        )

    def context(self, dag_id, item=None, ds=defaults.EXECUTION_DATE, external_trigger=False):
        return {
            'dag_run': self.dag_run(dag_id, external_trigger),
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

    def execute(self, task_id, item=None, dag_id='test_dag', external_trigger=False):
        op = self.get_operator(task_id, item)
        op.execute(self.context(dag_id, item, external_trigger=external_trigger))
        return self

    def test_cleanup_operator(self):
        self.build_dags().execute('cleanup_test_aggregation')
        self.engine.cleanup.assert_called_with(
            defaults.TMP_TABLE_NAME,
        )

    def test_cleanup_operator_with_parametrization(self):
        self.conf.parameterize_context()
        self.build_dags().execute('cleanup_test_aggregation', 'item')
        self.engine.cleanup.assert_called_with(
            defaults.TMP_ITEM_TABLE_NAME,
        )

    def test_cleanup_operator_with_external_trigger(self):
        self.build_dags().execute('cleanup_test_aggregation', external_trigger=True)
        self.engine.cleanup.assert_called_with(
            defaults.EXTERNAL_TMP_TABLE_NAME,
        )

    def test_aggregation_operator_without_parameterization(self):
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            {'test_time_key', 'another_table_test_src_column', 'test_src_column', 'key_column',
             'another_test_src_column'},
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-08-01' AND '2017-08-01'",
            None,
        )

    def test_aggregation_operator_with_timeseries(self):
        self.conf.with_timeseries()
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            {'test_time_key', 'another_table_test_src_column', 'test_src_column', 'key_column',
             'another_test_src_column'},
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-08-01' AND '2017-08-01'",
            None,
        )

    def test_aggregation_operator_with_other_timeseries(self):
        self.conf.with_timeseries(target_id='another_table')
        self.build_dags().execute('aggregate_another_aggregation')
        self.engine.aggregate.assert_called_with(
            'test_dag_agg_another_aggregation_20170801',
            {'another_aggregation_test_src_column', 'key_column', 'another_test_time_key'},
            u"SELECT everything FROM here",
            None,
        )

    def test_aggregation_operator_with_parameterized_context(self):
        self.conf.parameterize_context()
        self.build_dags().execute('aggregate_test_aggregation', 'item')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_ITEM_TABLE_NAME,
            {'another_table_test_src_column', 'item_column', 'test_time_key', 'test_src_column', 'key_column',
             'another_test_src_column'},
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-08-01' AND '2017-08-01'",
            {'item_column': 'item'},
        )

    def test_aggregation_operator_with_parameterized_context_and_aggregation(self):
        self.conf.parameterize_context().parameterize_aggregation()
        self.build_dags().execute('aggregate_test_aggregation', 'item')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_ITEM_TABLE_NAME,
            {'another_table_test_src_column', "'item' as item_column", 'test_time_key', 'test_src_column', 'key_column',
             'another_test_src_column'},
            u"SELECT * FROM item WHERE dt BETWEEN '2017-08-01' AND '2017-08-01'",
            None,
        )

    def test_aggregation_operator_with_offset(self):
        self.conf.with_offset()
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            {'test_time_key', 'another_table_test_src_column', 'test_src_column', 'key_column',
             'another_test_src_column'},
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-07-31' AND '2017-07-31'",
            None,
        )

    def test_aggregation_operator_with_reruns(self):
        self.conf.with_reruns()
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            {'test_time_key', 'another_table_test_src_column', 'test_src_column', 'key_column',
             'another_test_src_column'},
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-07-29' AND '2017-08-01'",
            None,
        )

    def test_aggregation_operator_with_offset_and_reruns(self):
        self.conf.with_offset().with_reruns()
        self.build_dags().execute('aggregate_test_aggregation')
        self.engine.aggregate.assert_called_with(
            defaults.TMP_TABLE_NAME,
            {'test_time_key', 'another_table_test_src_column', 'test_src_column', 'key_column',
             'another_test_src_column'},
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2017-07-28' AND '2017-07-31'",
            None,
        )

    def test_aggregation_operator_with_external_trigger(self):
        self.build_dags().execute('aggregate_test_aggregation', external_trigger=True)
        self.engine.aggregate.assert_called_with(
            defaults.EXTERNAL_TMP_TABLE_NAME,
            {'test_time_key', 'another_table_test_src_column', 'test_src_column', 'key_column',
             'another_test_src_column'},
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2016-08-01' AND '2016-09-01'",
            None,
        )

    def test_aggregation_operator_with_external_trigger_reruns_and_offset(self):
        self.conf.with_offset().with_reruns()
        self.build_dags().execute('aggregate_test_aggregation', external_trigger=True)
        self.engine.aggregate.assert_called_with(
            defaults.EXTERNAL_TMP_TABLE_NAME,
            {'test_time_key', 'another_table_test_src_column', 'test_src_column', 'key_column',
             'another_test_src_column'},
            u"SELECT * FROM DUAL WHERE dt BETWEEN '2016-07-28' AND '2016-08-31'",
            None,
        )

    def test_merge_operator_bootstrap(self):
        self.conf.parameterize_context()
        self.engine.describe.return_value = defaults.DESCRIBE_SRC_COLUMNS
        self.build_dags().execute('merge_test_aggregation_test_table', 'item')
        self.engine.describe.assert_called_with(defaults.TMP_ITEM_TABLE_NAME)
        self.engine.bootstrap.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                                 defaults.DESCRIBE_TARGET_COLUMNS_WITH_META)

    def test_merge_operator_bootstrap_with_timeseries(self):
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

    def test_merge_operator_delete_existing_data_with_timeseries_and_external_trigger(self):
        self.conf.with_timeseries()
        self.build_dags().execute('merge_test_aggregation_test_table', external_trigger=True)
        self.engine.delete_timeseries.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                                         defaults.TARGET_VALUE_COLUMNS,
                                                         {defaults.TIMESERIES_KEY: defaults.EXTERNAL_DATE_RANGE})

    def test_merge_operator_delete_existing_data_with_timeseries_offsets_reruns_and_external_trigger(self):
        self.conf.with_timeseries().with_reruns().with_offset()
        self.build_dags().execute('merge_test_aggregation_test_table', external_trigger=True)
        self.engine.delete_timeseries.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                                         defaults.TARGET_VALUE_COLUMNS,
                                                         {defaults.TIMESERIES_KEY: ('2016-07-28', '2016-08-31')})

    def test_merge_operator_merge(self):
        self.build_dags().execute('merge_test_aggregation_test_table')
        self.engine.merge.assert_called_with(defaults.TMP_TABLE_NAME, defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                             {'key_column': 'key_column'}, defaults.MERGE_VALUE_COLUMNS, defaults.MERGE_UPDATE_TYPES, 'test_time_key')

    def test_merge_operator_merge_with_parametrization(self):
        self.conf.parameterize_context()
        self.build_dags().execute('merge_test_aggregation_test_table', 'item')
        self.engine.merge.assert_called_with(defaults.TMP_ITEM_TABLE_NAME, defaults.TARGET_SCHEMA_NAME,
                                             defaults.TARGET_NAME, {'key_column': 'key_column', 'item_column': 'item_column'},
                                             defaults.MERGE_VALUE_COLUMNS, defaults.MERGE_UPDATE_TYPES, 'test_time_key')

    def test_merge_operator_merge_with_timeseries(self):
        self.conf.with_timeseries()
        self.build_dags().execute('merge_test_aggregation_test_table')
        self.engine.merge.assert_called_with(defaults.TMP_TABLE_NAME, defaults.TARGET_SCHEMA_NAME,
                                             defaults.TARGET_NAME, {'key_column': 'key_column', 'timeseries_column': 'test_time_key'},
                                             defaults.MERGE_VALUE_COLUMNS, None, None)

    def test_merge_operator_merge_with_timeseries_and_parametrization(self):
        self.conf.parameterize_context().with_timeseries()
        self.build_dags().execute('merge_test_aggregation_test_table', 'item')
        self.engine.merge.assert_called_with(defaults.TMP_ITEM_TABLE_NAME, defaults.TARGET_SCHEMA_NAME,
                                             defaults.TARGET_NAME,
                                             {'key_column': 'key_column', 'timeseries_column': 'test_time_key', 'item_column': 'item_column'},
                                             defaults.MERGE_VALUE_COLUMNS, None, None)

    def test_merge_operator_merge_with_external_trigger(self):
        self.build_dags().execute('merge_test_aggregation_test_table', external_trigger=True)
        self.engine.merge.assert_called_with(defaults.EXTERNAL_TMP_TABLE_NAME, defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                             {'key_column': 'key_column'}, defaults.MERGE_VALUE_COLUMNS, defaults.MERGE_UPDATE_TYPES, 'test_time_key')

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

    def test_finish_operator_purge_with_timeseries_and_external_trigger(self):
        self.conf.with_timeseries()
        self.build_dags().execute('finish_test_table', external_trigger=True)
        self.engine.purge.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                             defaults.TARGET_ALL_VALUE_COLUMNS,
                                             {defaults.TIMESERIES_KEY: defaults.EXTERNAL_DATE_RANGE})

    def test_finish_operator_purge_with_timeseries_reruns_offsets_and_external_trigger(self):
        self.conf.with_timeseries().with_offset().with_reruns()
        self.build_dags().execute('finish_test_table', external_trigger=True)
        self.engine.purge.assert_called_with(defaults.TARGET_SCHEMA_NAME, defaults.TARGET_NAME,
                                             defaults.TARGET_ALL_VALUE_COLUMNS,
                                             {defaults.TIMESERIES_KEY: ('2016-07-28', '2016-09-01')})

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
