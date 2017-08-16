from airflow.models import BaseOperator
from datetime import datetime, timedelta

from karajan.config import Config


class KarajanBaseOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(KarajanBaseOperator, self).__init__(*args, **kwargs)

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


class KarajanAggregateOperator(KarajanBaseOperator):
    ui_color = '#f9baba'

    def __init__(self, aggregation, columns, *args, **kwargs):
        self.aggregation = aggregation
        self.columns = columns
        task_id = "aggregate_%s" % aggregation.name
        super(KarajanAggregateOperator, self).__init__(*args, task_id=task_id, **kwargs)

    def execute(self, context):
        self.set_execution_dates(context)
        query = Config.render(self.aggregation.query, self.params)

        # set where and selected columns based on parametrization level
        columns = self.columns
        where = None
        if self.params.get('item'):
            item = self.params['item']
            item_column = self.params['item_column']
            if self.aggregation.parameterize:
                columns = [n if n != item_column else "'%s' as %s" % (item, item_column) for n in
                           columns]
            else:
                where = {item_column: item}

        self.engine.aggregate(self.tmp_table_name(context), columns, query, where)


class KarajanCleanupOperator(KarajanBaseOperator):
    ui_color = '#4255ff'

    def __init__(self, aggregation, *args, **kwargs):
        self.aggregation = aggregation
        task_id = "cleanup_%s" % aggregation.name
        super(KarajanCleanupOperator, self).__init__(*args, task_id=task_id, **kwargs)

    def execute(self, context):
        self.engine.cleanup(self.tmp_table_name(context))


class KarajanMergeOperator(KarajanBaseOperator):
    ui_color = '#99F6F7'

    def __init__(self, aggregation, target, *args, **kwargs):
        self.aggregation = aggregation
        self.target = target
        task_id = "merge_%s_%s" % (aggregation.name, target.name)
        super(KarajanMergeOperator, self).__init__(*args, task_id=task_id, **kwargs)

    def execute(self, context):
        tmp_table_name = self.tmp_table_name(context)
        schema_name = self.target.schema
        table_name = self.target.name

        # get source column definitions from tmp table
        src_columns = self.engine.describe(tmp_table_name)
        # map source column definitions to target columns
        columns = {ac.name: src_columns[ac.src_column_name] for ac in
                   self.target.aggregated_columns(self.aggregation.name).values()}
        for kc in self.target.key_columns:
            columns[kc] = src_columns[kc]
        if self.target.is_timeseries():
            columns[self.target.timeseries_key] = src_columns[self.aggregation.time_key]
        else:
            for ac in self.target.aggregated_columns(self.aggregation.name).values():
                if ac.depends_on_past():
                    columns['_%s_updated_at' % ac.name] = 'DATE'

        # bootstrap table and columns
        self.engine.bootstrap(schema_name, table_name, columns)

        # delete existing timeseries data
        if self.target.is_timeseries():
            timeseries_column = self.target.timeseries_key
            date_range = self.set_execution_dates(context)
            value_columns = self.target.aggregated_columns(self.aggregation.name).keys()
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
        else:
            update_types = {ac.name: ac.update_type for ac in
                            self.target.aggregated_columns(self.aggregation.name).values()}

        self.engine.merge(tmp_table_name, schema_name, table_name, key_columns, value_columns, update_types)


class KarajanFinishOperator(KarajanBaseOperator):
    ui_color = '#c8e29e'

    def __init__(self, target, retrospec, *args, **kwargs):
        self.target = target
        self.retrospec = retrospec
        task_id = "finish_%s" % target.name
        super(KarajanFinishOperator, self).__init__(*args, task_id=task_id, **kwargs)

    def execute(self, context):
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
