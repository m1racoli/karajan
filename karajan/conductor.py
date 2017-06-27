from airflow.models import DAG

from karajan.config import Config
from karajan.engines import BaseEngine
from karajan.model import AggregatedTable, AggregatedColumn, Column, NothingDependency, get_dependency


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
        done_op = engine.done_operator('done', dag)
        merge = engine.merge_operator('merge', dag, table)
        if table.timeseries_key is not None:
            clear_time_unit = engine.clear_time_unit_operator('clear_%s' % table.timeseries_key, dag, table)
            clear_time_unit.set_downstream(merge)
            after_agg = clear_time_unit
        else:
            after_agg = merge
        cleanup = engine.cleanup_operator('cleanup', dag, table)
        cleanup.set_upstream(merge)
        cleanup.set_downstream(done_op)
        dep_ops = {}
        col_ops = {}

        agg_columns = self._agg_columns(table)
        for col_id, col in agg_columns.iteritems():
            col_op = engine.aggregation_operator(col_id, dag, table, col)
            col_ops[col_id] = col_op
            col_op.set_downstream(after_agg)

            for params in table.param_set():

                for dep in self._get_dependencies(col, params):
                    dep_id = dep.id()
                    if dep_id not in dep_ops:
                        dep_op = engine.dependency_operator(dep_id, dag, dep)
                        dep_ops[dep_id] = dep_op
                        dep_op.set_upstream(init)
                    col_op.set_upstream(dep_ops[dep_id])

        return dag

    def _columns(self, table):
        return {k: Column(k, self.conf['columns'][v]) for k, v in table.aggregated_columns.iteritems()}

    def _agg_columns(self, table):
        cols = {}
        for column_name, conf_name in table.aggregated_columns.iteritems():
            col = AggregatedColumn(column_name, self.conf['columns'][conf_name], table)
            cols[col.id()] = col
        return cols

    @staticmethod
    def _get_dependencies(column, params):
        if column.has_dependencies():
            return [get_dependency(Config.render(dep_conf, params)) for dep_conf in column.dependencies]
        else:
            return [NothingDependency()]
