from airflow.models import DAG

from karajan.config import Config
from karajan.engines import BaseEngine
from karajan.model import AggregatedTable, AggregatedColumn, Column


class Conductor(object):
    def __init__(self, conf=None, prefix=''):
        """
        :param conf: path or dict
        :param prefix: dag_id = {prefix}_{table_name}
        """
        self.conf = Config.load(conf)
        self.prefix = prefix

    def build(self, engine=BaseEngine(), target=None):

        tables = {n: AggregatedTable(n, c) for n, c in self.conf['tables'].iteritems()}

        dags = {}

        for table in tables.values():
            dag = self._build_dag(engine, table)
            dags[dag.dag_id] = dag

        if target:
            target.update(dags)

        return dags

    def _build_dag(self, engine, table):
        dag = DAG(table.dag_id(self.prefix), start_date=table.start_date)
        init = engine.init_operator('init', dag, table, self._columns(table))
        merge = engine.merge_operator('merge', dag, table)
        if table.timeseries_key is not None:
            clear_time_unit = engine.clear_time_unit_operator('clear_%s' % table.timeseries_key, dag, table)
            clear_time_unit.set_downstream(merge)
            after_agg = clear_time_unit
        else:
            after_agg = merge
        cleanup = engine.cleanup_operator('cleanup', dag, table)
        cleanup.set_upstream(merge)
        deps = {}
        cols = {}

        for item in table.items:

            params = {}
            params.update(table.defaults)
            params.update(item)
            params['item_key'] = params.get(table.item_key)

            columns = self._agg_columns(table, params)

            for dep_id, dep in self._merge_dependencies(columns).iteritems():
                if dep_id not in deps:
                    d_op = engine.dependency_operator(dep_id, dag, dep)
                    deps[dep_id] = d_op
                    d_op.set_upstream(init)

            for col_id, col in columns.iteritems():
                if col_id not in cols:
                    col_op = engine.aggregation_operator(col_id, dag, table, col)
                    cols[col_id] = col_op
                    col_op.set_downstream(after_agg)

                for dep_id in col.dependency_ids():
                    dep_op = deps[dep_id]
                    if col_id not in dep_op.downstream_task_ids:
                        cols[col_id].set_upstream(dep_op)

        return dag

    def _columns(self, table):
        return {k: Column(k, self.conf['columns'][v]) for k, v in table.aggregated_columns.iteritems()}

    def _agg_columns(self, table, params):
        cols = {}
        for column_name, conf_name in table.aggregated_columns.iteritems():
            col = AggregatedColumn(column_name, self.conf['columns'][conf_name], params)
            cols[col.id()] = col
        return cols

    @staticmethod
    def _merge_dependencies(columns):
        deps = {}
        for column in columns.values():
            for dep in column.dependencies:
                deps[dep.id()] = dep
        return deps
