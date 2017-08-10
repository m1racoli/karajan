from airflow.models import BaseOperator
from datetime import datetime, timedelta

from karajan.config import Config


class KarajanBaseOperator(BaseOperator):
    def __init__(self, engine, *args, **kwargs):
        self.engine = engine
        super(KarajanBaseOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        raise NotImplementedError()

    def tmp_table_name(self, context):
        return "%s_agg_%s_%s" % (context['dag'].dag_id, self.aggregation.name, context['ds_nodash'])

    def set_execution_dates(self, context):
        # ds = context['ds']
        ds = datetime.strptime(context['ds'], "%Y-%m-%d")
        ds_start = ds if (self.aggregation.reruns + self.aggregation.offset == 0) else ds - timedelta(
            days=self.aggregation.reruns + self.aggregation.offset)
        ds_end = ds if self.aggregation.offset == 0 else ds - timedelta(days=self.aggregation.offset)
        self.params['start_date'] = ds_start.strftime("%Y-%m-%d")
        self.params['end_date'] = ds_end.strftime("%Y-%m-%d")


class KarajanAggregateOperator(KarajanBaseOperator):
    ui_color = '#f9baba'

    def __init__(self, aggregation, columns, params, *args, **kwargs):
        self.aggregation = aggregation
        self.columns = columns
        task_id = "aggregate_%s" % aggregation.name
        super(KarajanAggregateOperator, self).__init__(*args, task_id=task_id, params=params, **kwargs)

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
class KarajanCleanOperator(KarajanBaseOperator):
    ui_color = '#4255ff'

    def __init__(self, *args, **kwargs):
        super(KarajanCleanOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.engine.clean(self.tmp_table_name(context))


