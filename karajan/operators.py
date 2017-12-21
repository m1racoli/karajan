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

import logging
from airflow.models import BaseOperator, TaskInstance
from datetime import datetime, timedelta

from karajan.config import Config


class KarajanBaseOperator(BaseOperator):
    def __init__(self, dag=None, *args, **kwargs):
        # some attributes of the task are inferred by the egnine coming from the DAG
        kwargs.update(**dag.engine.task_attributes)
        super(KarajanBaseOperator, self).__init__(*args, dag=dag, **kwargs)

    @property
    def engine(self):
        return self.dag.engine

    def execute(self, context):
        raise NotImplementedError()

    def tmp_table_name(self, context):
        dag_run = context['dag_run']
        if dag_run.external_trigger:
            execution_date = dag_run.execution_date.strftime("%Y%m%dT%H%M%S")
        else:
            execution_date = context['ds_nodash']
        return "%s_agg_%s_%s" % (context['dag'].dag_id, self.aggregation.name, execution_date)

    def set_execution_dates(self, context, retrospec=None):
        dag_run = context['dag_run']
        if dag_run.external_trigger:
            conf = dag_run.conf
            ds_start = conf['start_date']
            ds_end = conf['end_date']
        else:
            ds_start = datetime.strptime(context['ds'], "%Y-%m-%d")
            ds_end = datetime.strptime(context['ds'], "%Y-%m-%d")

        if retrospec is not None:
            ds_start = ds_start - timedelta(days=retrospec)
        else:
            ds_start = ds_start - timedelta(days=self.aggregation.reruns + self.aggregation.offset)
            ds_end = ds_end - timedelta(days=self.aggregation.offset)
        self.params['start_date'] = ds_start.strftime("%Y-%m-%d")
        self.params['end_date'] = ds_end.strftime("%Y-%m-%d")
        return self.params['start_date'], self.params['end_date']

    @staticmethod
    def is_not_in_limit(context, *names):
        conf = context['dag_run'].conf
        if conf is None:
            return None
        limit = conf.get('limit')
        return limit and any(name not in limit for name in names)

    @staticmethod
    def limited_columns(context, name, columns):
        conf = context['dag_run'].conf
        if conf is None:
            return columns
        limit = conf.get('limit')
        if not limit:
            return columns
        column_limit = limit[name]
        if not column_limit:
            return columns

        if isinstance(columns, dict):
            return {c: v for c,v in columns.iteritems() if c in column_limit}

        return [c for c in columns if c in column_limit]


class KarajanDependencyOperator(KarajanBaseOperator):
    ui_color = '#40c435'

    def __init__(self, op, *args, **kwargs):
        self.op = op
        super(KarajanDependencyOperator, self).__init__(*args, task_id=op.task_id, **kwargs)

    def execute(self, context):
        if context['dag_run'].external_trigger:
            logging.info("skipping dependency check due to external run")
            return
        ti = TaskInstance(self.op, context['execution_date'])
        # the task is not supposed to have a DAG, but for the rendering we need access to the DAG
        setattr(ti.task, '_dag', self.dag)
        ti.render_templates()
        ti.task.execute(context)


class KarajanAggregateOperator(KarajanBaseOperator):
    ui_color = '#f9baba'

    def __init__(self, aggregation, columns, *args, **kwargs):
        self.aggregation = aggregation
        self.columns = columns
        task_id = "aggregate_%s" % aggregation.name
        super(KarajanAggregateOperator, self).__init__(*args, task_id=task_id, **kwargs)

    def execute(self, context):
        if self.is_not_in_limit(context, self.aggregation.name):
            logging.info("not part of limited run")
            return

        self.set_execution_dates(context)
        query = Config.render(self.aggregation.query, self.params)

        # filter columns by limit
        columns = self.limited_columns(context, self.aggregation.name, self.columns)

        # get us all required columns and transformations
        columns, transformations = self.aggregation.transformation_upstream_columns(columns)

        # set where and selected columns based on parametrization level
        where = None
        if self.params.get('item'):
            item = self.params['item']
            item_column = self.params['item_column']
            if self.aggregation.parameterize:
                columns = {n if n != item_column else "'%s' as %s" % (item, item_column) for n in
                           columns}
            else:
                where = {item_column: item}

        # cleanup potentially existing table in case of retries
        self.engine.cleanup(self.tmp_table_name(context))

        self.engine.aggregate(self.tmp_table_name(context), columns, query, where)

        for transformation in transformations:
            self.engine.apply_transformation(self.tmp_table_name(context), transformation, self.params)


class KarajanCleanupOperator(KarajanBaseOperator):
    ui_color = '#4255ff'

    def __init__(self, aggregation, *args, **kwargs):
        self.aggregation = aggregation
        task_id = "cleanup_%s" % aggregation.name
        super(KarajanCleanupOperator, self).__init__(*args, task_id=task_id, **kwargs)

    def execute(self, context):
        if self.is_not_in_limit(context, self.aggregation.name):
            logging.info("not part of limited run")
            return

        self.engine.cleanup(self.tmp_table_name(context))


class KarajanMergeOperator(KarajanBaseOperator):
    ui_color = '#99F6F7'

    def __init__(self, aggregation, target, *args, **kwargs):
        self.aggregation = aggregation
        self.target = target
        task_id = "merge_%s_%s" % (aggregation.name, target.name)
        super(KarajanMergeOperator, self).__init__(*args, task_id=task_id, **kwargs)

    def execute(self, context):
        if self.is_not_in_limit(context, self.aggregation.name, self.target.name):
            logging.info("not part of limited run")
            return
        tmp_table_name = self.tmp_table_name(context)
        schema_name = self.target.schema
        table_name = self.target.name

        # get source column definitions from tmp table
        src_columns_defs = self.engine.describe(tmp_table_name)
        # map source column definitions to target columns
        columns_defs = {ac.name: src_columns_defs[ac.src_column_name] for ac in
                   self.limited_columns(context, self.target.name, self.target.aggregated_columns(self.aggregation.name)).values()}
        for kc in self.target.key_columns:
            columns_defs[kc] = src_columns_defs[kc]
        if self.target.is_timeseries():
            columns_defs[self.target.timeseries_key] = src_columns_defs[self.aggregation.time_key]
        else:
            for ac in self.target.aggregated_columns(self.aggregation.name).values():
                if ac.depends_on_past():
                    columns_defs['_%s_updated_at' % ac.name] = 'DATE'

        # bootstrap table and columns
        self.engine.bootstrap(schema_name, table_name, columns_defs)

        # delete existing timeseries data
        if self.target.is_timeseries():
            timeseries_column = self.target.timeseries_key
            date_range = self.set_execution_dates(context)
            value_columns = self.limited_columns(context, self.target.name, self.target.aggregated_columns(self.aggregation.name).keys())
            where = {timeseries_column: date_range}
            if self.params.get('item'):
                where[self.params.get('item_column')] = self.params.get('item')
            self.engine.delete_timeseries(schema_name, table_name, value_columns, where)

        # merge
        value_columns = {ac.name: ac.src_column_name for ac in
                         self.target.aggregated_columns(self.aggregation.name).values()}
        key_columns = {k: k for k in self.target.key_columns}
        if self.target.is_timeseries():
            key_columns[self.target.timeseries_key] = self.aggregation.time_key
            update_types = None
            time_key = None
        else:
            update_types = {ac.name: ac.update_type for ac in
                            self.target.aggregated_columns(self.aggregation.name).values()}
            time_key = self.aggregation.time_key

        value_columns = self.limited_columns(context, self.target.name, value_columns)
        self.engine.merge(tmp_table_name, schema_name, table_name, key_columns, value_columns, update_types, time_key)


class KarajanFinishOperator(KarajanBaseOperator):
    ui_color = '#c8e29e'

    def __init__(self, target, retrospec, *args, **kwargs):
        self.target = target
        self.retrospec = retrospec
        task_id = "finish_%s" % target.name
        super(KarajanFinishOperator, self).__init__(*args, task_id=task_id, **kwargs)

    def execute(self, context):
        if self.is_not_in_limit(context, self.target.name):
            logging.info("not part of limited run")
            return

        schema_name = self.target.schema
        table_name = self.target.name

        if self.target.is_timeseries():
            date_range = self.set_execution_dates(context, self.retrospec)
            where = {self.target.timeseries_key: date_range}
            if self.params.get('item'):
                where[self.params.get('item_column')] = self.params.get('item')
            self.engine.purge(schema_name, table_name, self.target.aggregated_columns().keys(), where)

        if self.target.has_parameter_columns():
            if self.params.get('item'):
                where = {self.params.get('item_column'): self.params.get('item')}
            else:
                where = None
            parameter_columns = {c: self.params[p] for c, p in self.target.parameter_columns.iteritems()}
            self.engine.parameters(schema_name, table_name, parameter_columns, where)
