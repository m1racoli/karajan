from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

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
        self.targets = [Target(n, c, self.context) for n, c in self.conf['targets'].iteritems()]
        self.aggregations = {n: Aggregation(n, c, self.context) for n, c in self.conf['aggregations'].iteritems()}

    def build(self, dag_id, engine=BaseEngine(), output=None, import_subdags=False):

        if not self.targets:
            return {}

        dag = DAG(dag_id=dag_id, start_date=min(t.start_date for t in self.targets))

        DummyOperator(task_id='init', dag=dag)
        DummyOperator(task_id='done', dag=dag)

        for item, params in self.context.params().iteritems():
            self._build_subdag(item, params, engine, dag)

        dags = {dag_id: dag}

        if self.context.is_parameterized() and import_subdags:
            for t in dag.tasks:
                if isinstance(t, SubDagOperator):
                    d = t.dag
                    dags[d.dag_id] = d

        if output is not None:
            output.update(dags)

        return dags

    def _build_subdag(self, item, params, engine, parent_dag):
        if item:
            # parametrization, so we use sub dags per item
            dag = DAG(dag_id="%s.%s" % (parent_dag.dag_id, item), start_date=parent_dag.start_date)
            subdag_op = SubDagOperator(task_id=item, subdag=dag, dag=parent_dag)
            subdag_op.set_upstream(parent_dag.get_task('init'))
            subdag_op.set_downstream(parent_dag.get_task('done'))
            init = DummyOperator(task_id='init', dag=dag)
            done = DummyOperator(task_id='done', dag=dag)
        else:
            # no parametrization, so we use the main dag
            dag = parent_dag
            init = dag.get_task('init')
            done = dag.get_task('done')

        targets = [t for t in self.targets if t.has_item(item)]
        aggregations = [self.aggregations[a] for a in {a for t in targets for a in t.aggregations}]
        dependency_operators = {}
        purge_operators = {}

        for target in targets:
            purge_operator = engine.purge_operator(dag, target, item)
            purge_operators[target.name] = purge_operator
            purge_operator.set_downstream(done)
            if target.has_parameter_columns():
                param_col_op = engine.param_column_op(dag, target, params, item)
                param_col_op.set_upstream(purge_operator)
                param_col_op.set_downstream(done)

        for aggregation in aggregations:
            src_column_names = {c for t in targets for c in t.src_column_names(aggregation.name)}
            aggregation_operator = engine.aggregation_operator(dag, src_column_names, aggregation, params, item)
            for dependency in self._get_dependencies(aggregation, params):
                dep_id = dependency.id()
                if dep_id not in dependency_operators:
                    dependency_operator = engine.dependency_operator(dep_id, dag, dependency)
                    dependency_operators[dep_id] = dependency_operator
                    dependency_operator.set_upstream(init)
                aggregation_operator.set_upstream(dependency_operators[dep_id])

            clean_operator = engine.cleanup_operator(dag, aggregation, item)
            clean_operator.set_downstream(done)

            for target in targets:
                if aggregation.name not in target.aggregations:
                    continue

                prepare_operator = engine.prepare_operator(dag, aggregation, target, item)
                prepare_operator.set_upstream(aggregation_operator)

                merge_operator = engine.merge_operator(dag, aggregation, target, item)
                merge_operator.set_upstream(prepare_operator)
                merge_operator.set_downstream(clean_operator)
                merge_operator.set_downstream(purge_operators[target.name])

        return dag

    @staticmethod
    def _get_dependencies(agg, params):
        if agg.has_dependencies():
            return [get_dependency(Config.render(dep_conf, params)) for dep_conf in agg.dependencies]
        else:
            return [NothingDependency()]
