from airflow.models import DAG

from karajan.config import Config
from karajan.engines import BaseEngine
from karajan.model import Target, Aggregation, Context
from karajan.dependencies import NothingDependency, get_dependency


class Conductor(object):
    def __init__(self, conf=None):
        """
        :param conf: path or dict
        """
        self.conf = Config.load(conf)
        self.context = Context(self.conf['context'])
        self.targets = {n: Target(n, c, self.context) for n, c in self.conf['targets'].iteritems()}

    def build(self, prefix='', engine=BaseEngine(), output=None):

        dags = {}

        for target in self.targets.values():
            dag = self._build_dag(prefix, engine, target)
            dags[dag.dag_id] = dag

        if output:
            output.update(dags)

        return dags

    def _build_dag(self, prefix, engine, target):
        dag = DAG(target.dag_id(prefix), start_date=target.start_date)
        init = engine.init_operator('init', dag, target)
        done_op = engine.done_operator('done', dag)
        dep_ops = {}

        for agg_id, agg_columns in target.aggregations.iteritems():
            agg = Aggregation(agg_id, self.conf['aggregations'][agg_id], agg_columns, self.context)
            agg_op = engine.aggregation_operator('aggregate_%s' % agg_id, dag, target, agg, self.context)

            for params in self.context.params(target).values():
                for dep in self._get_dependencies(agg, params):
                    dep_id = dep.id()
                    if dep_id not in dep_ops:
                        dep_op = engine.dependency_operator(dep_id, dag, dep)
                        dep_ops[dep_id] = dep_op
                        dep_op.set_upstream(init)
                    if dep_id not in agg_op.upstream_task_ids:
                        agg_op.set_upstream(dep_ops[dep_id])

            merge_op = engine.merge_operator('merge_%s' % agg_id, dag, target, agg)
            merge_op.set_upstream(agg_op)

            clean_op = engine.cleanup_operator('cleanup_%s' % agg_id, dag, target, agg)
            clean_op.set_upstream(merge_op)
            clean_op.set_downstream(done_op)

        return dag

    @staticmethod
    def _get_dependencies(agg, params):
        if agg.has_dependencies():
            return [get_dependency(Config.render(dep_conf, params)) for dep_conf in agg.dependencies]
        else:
            return [NothingDependency()]
