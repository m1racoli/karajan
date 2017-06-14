from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator


class BaseEngine(object):
    def __init__(self, exasol_conn_id=None):
        self.exasol_conn_id = exasol_conn_id

    def init_operator(self, task_id, dag, table):
        return DummyOperator(task_id=task_id, dag=dag)

    def dependency_operator(self, task_id, dag, dep):
        return DummyOperator(task_id=task_id, dag=dag)

    def aggregation_operator(self, task_id, dag, table, column):
        return DummyOperator(task_id=task_id, dag=dag)

    def merge_operator(self, task_id, dag, table):
        return DummyOperator(task_id=task_id, dag=dag)

    def cleanup_operator(self, task_id, dag, table):
        return DummyOperator(task_id=task_id, dag=dag)
