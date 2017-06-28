from airflow.models import DAG

from karajan.config import Config
from karajan.engines import BaseEngine
from karajan.model import AggregatedTable, Aggregation, Column, NothingDependency, get_dependency


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
        init = engine.init_operator('init', dag, table)
        done_op = engine.done_operator('done', dag)
        dep_ops = {}

        for agg_id, agg_columns in table.aggregations.iteritems():
            agg = self._aggregation(agg_id, table)
            agg_op = engine.aggregation_operator('aggregate_%s' % agg_id, dag, table, agg)

            for params in table.param_set():
                for dep in self._get_dependencies(agg, params):
                    dep_id = dep.id()
                    if dep_id not in dep_ops:
                        dep_op = engine.dependency_operator(dep_id, dag, dep)
                        dep_ops[dep_id] = dep_op
                        dep_op.set_upstream(init)
                    agg_op.set_upstream(dep_ops[dep_id])

            merge_op = engine.merge_operator('merge_%s' % agg_id, dag, table, agg)
            merge_op.set_upstream(agg_op)

            clean_op = engine.cleanup_operator('cleanup_%s' % agg_id, dag, table, agg)
            clean_op.set_upstream(merge_op)
            clean_op.set_downstream(done_op)

        return dag

    def _aggregation(self, agg_id, table):
        return Aggregation(agg_id, self.conf['aggregations'][agg_id], table)

    @staticmethod
    def _get_dependencies(agg, params):
        if agg.has_dependencies():
            return [get_dependency(Config.render(dep_conf, params)) for dep_conf in agg.dependencies]
        else:
            return [NothingDependency()]
