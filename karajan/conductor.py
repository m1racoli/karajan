from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from karajan.dependencies import NothingDependency, get_dependency, TargetDependency
from karajan.engines import BaseEngine
from karajan.model import Target, Aggregation, Context, KarajanDAG
from karajan.operators import *


class Conductor(object):
    def __init__(self, conf=None):
        """
        :param conf: path or dict
        """
        self.conf = Config.load(conf)
        self.context = Context(self.conf['context'])
        self.targets = {n: Target(n, c, self.context) for n, c in self.conf['targets'].iteritems()}
        self.aggregations = {n: Aggregation(n, c, self.context) for n, c in self.conf['aggregations'].iteritems()}

    def build(self, dag_id, engine=BaseEngine(), output=None):

        if not self.targets:
            return {}

        dags = {}

        for item, params in self.context.params().iteritems():
            item_start_date = min(t.start_date for t in self.targets.values() if t.has_item(item))
            dag = KarajanDAG(dag_id, engine, item, start_date=item_start_date)
            self._build_subdag(params, dag)
            dags[dag.dag_id] = dag

        if output is not None:
            output.update(dags)

        return dags

    def _build_subdag(self, params, dag):
        init = DummyOperator(task_id='init', dag=dag)
        done = DummyOperator(task_id='done', dag=dag)

        targets = [t for t in self.targets.values() if t.has_item(dag.item)]
        aggregations = [self.aggregations[a] for a in {a for t in targets for a in t.aggregations}]
        aggregation_operators = {}
        merge_operators = {}
        dependency_operators = {}
        target_dependencies = {}
        finish_operators = {}

        for target in targets:
            # for the purge operation we need the maximum number of days we will look back = offset + reruns
            retrospec = max(agg.retrospec() for agg in aggregations if agg.name in target.aggregations)
            finish_operator = KarajanFinishOperator(
                dag=dag,
                params=params,
                target=target,
                retrospec=retrospec
            )
            finish_operators[target.name] = finish_operator
            finish_operator.set_downstream(done)

        for aggregation in aggregations:
            src_column_names = list({c for t in targets for c in t.src_column_names(aggregation.name)})
            src_column_names.append(aggregation.time_key)
            aggregation_operator = KarajanAggregateOperator(
                aggregation=aggregation,
                columns=src_column_names,
                params=params,
                dag=dag
            )

            aggregation_operators[aggregation.name] = aggregation_operator

            for dependency in self._get_dependencies(aggregation, params):
                if isinstance(dependency, TargetDependency):
                    target_dependencies[aggregation.name] = target_dependencies.get(aggregation.name, [])
                    target_dependencies[aggregation.name].append(dependency)
                    continue
                dep_id = dependency.id()
                if dep_id not in dependency_operators:
                    dependency_operator = dag.engine.dependency_operator(dep_id, dag, dependency)
                    dependency_operators[dep_id] = dependency_operator
                    dependency_operator.set_upstream(init)
                aggregation_operator.set_upstream(dependency_operators[dep_id])

            clean_operator = KarajanCleanupOperator(
                aggregation=aggregation,
                params=params,
                dag=dag
            )
            clean_operator.set_downstream(done)

            for target in targets:
                if aggregation.name not in target.aggregations:
                    continue

                merge_operator = KarajanMergeOperator(
                    aggregation=aggregation,
                    target=target,
                    params=params,
                    dag=dag,
                )
                merge_operators[(aggregation.name, target.name)] = merge_operator
                merge_operator.set_upstream(aggregation_operator)
                merge_operator.set_downstream(clean_operator)
                merge_operator.set_downstream(finish_operators[target.name])

        for aggregation_id, dependencies in target_dependencies.iteritems():
            aggregation_operator = aggregation_operators[aggregation_id]
            for target_dependency in dependencies:
                target = self.targets[target_dependency.target]
                if not target.has_item(dag.item):
                    # nothing to wait for for this item
                    continue
                for agg_id in target.aggregations_for_columns(target_dependency.columns):
                    aggregation_operator.set_upstream(merge_operators[(agg_id, target.name)])
        return dag

    @staticmethod
    def _get_dependencies(agg, params):
        if agg.has_dependencies():
            return [get_dependency(Config.render(dep_conf, params)) for dep_conf in agg.dependencies]
        else:
            return [NothingDependency()]
