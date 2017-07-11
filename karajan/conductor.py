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
        param_col_ops = {}

        for item, params in self.context.params(target).iteritems():
            if target.has_parameter_columns():
                param_col_op_name = "merge_parameter_columns_%s" % item if item else "merge_parameter_columns"
                param_col_op = engine.param_column_op(param_col_op_name, dag, target, params, item)
                param_col_ops[item] = param_col_op
                param_col_op.set_downstream(done_op)

        for agg_id, agg_columns in target.aggregations.iteritems():
            agg = Aggregation(agg_id, self.conf['aggregations'][agg_id], agg_columns, self.context)
            agg_ops = self.get_aggregation_operators(engine, dag, agg, target)

            for item, params in self.context.params(target).iteritems():
                for dep in self._get_dependencies(agg, params):
                    dep_id = dep.id()
                    if dep_id not in dep_ops:
                        dep_op = engine.dependency_operator(dep_id, dag, dep)
                        dep_ops[dep_id] = dep_op
                        dep_op.set_upstream(init)
                    self.agg_set_upstream_dep(agg_ops, dep_id, dep_ops, item)
                    self.agg_set_upstream_dep(agg_ops, dep_id, dep_ops)

            for item, agg_op in agg_ops.iteritems():
                name = agg_id if not item else '%s_%s' % (agg_id, item)
                merge_op = engine.merge_operator('merge_%s' % name, dag, target, agg)
                merge_op.set_upstream(agg_op)
                for i, param_col_op in param_col_ops.iteritems():
                    if not item or i == item:
                        merge_op.set_downstream(param_col_op)

                clean_op = engine.cleanup_operator('cleanup_%s' % name, dag, target, agg)
                clean_op.set_upstream(merge_op)
                clean_op.set_downstream(done_op)

        return dag

    @staticmethod
    def agg_set_upstream_dep(agg_ops, dep_id, dep_ops, item=''):
        if item in agg_ops:
            agg_op = agg_ops[item]
            if dep_id not in agg_op.upstream_task_ids:
                agg_op.set_upstream(dep_ops[dep_id])

    def get_aggregation_operators(self, engine, dag, aggregation, target):
        if self.context.is_parameterized():
            if aggregation.parameterize:
                return {
                    item: engine.aggregation_operator('aggregate_%s_%s' % (aggregation.name, item), dag, target, aggregation, params, (self.context.item_column, item)) for item, params in self.context.params(target).iteritems()
                }
            else:
                return {
                    '': engine.aggregation_operator('aggregate_%s' % aggregation.name, dag, target, aggregation, self.context.defaults, (self.context.item_column, target.items))
                }
        else:
            return {
                '': engine.aggregation_operator('aggregate_%s' % aggregation.name, dag, target, aggregation, self.context.defaults, None)
            }

    @staticmethod
    def _get_dependencies(agg, params):
        if agg.has_dependencies():
            return [get_dependency(Config.render(dep_conf, params)) for dep_conf in agg.dependencies]
        else:
            return [NothingDependency()]
