import sys

from behave import *
from karajan.conductor import Conductor
from assertions import *
from config import *


def get_dag(context, dag_id):
    dag = context.dags.get(dag_id)
    if dag:
        return dag
    raise Exception('DAG %s not found' % dag_id)


@when(u'I build the DAGs')
def step_impl(context):
    context.dags = Conductor(get_conf(context)).build()


@when(u'I try to build the DAGs')
def step_impl(context):
    try:
        context.dags = Conductor(get_conf(context)).build()
    except Exception:
        context.exception = {'type': sys.exc_info()[0].__name__, 'msg': sys.exc_info()[1]}
        context.dags = {}
        pass


@then(u'there should be no DAGs')
def step_impl(context):
    assert_equals(len(context.dags), 0)


@then(u'there should be one DAG')
def step_impl(context):
    assert_equals(len(context.dags), 1)


@then(u'there should be {cnt:d} DAGs')
def step_impl(context, cnt):
    assert_equals(len(context.dags), cnt)


@then(u'there should have been an exception {exception_type}')
def step_impl(context, exception_type):
    assert_contains(context, 'exception')
    assert_equals(context.exception.get('type'), exception_type)


@then(u'the DAG {dag_id} should have the task {task_id}')
def step_impl(context, dag_id, task_id):
    assert_contains(get_dag(context, dag_id).task_ids, task_id)


@then(u'the task {task_id} of {dag_id} should be of type {task_type}')
def step_impl(context, task_id, dag_id, task_type):
    assert_equals(type(get_dag(context, dag_id).task_dict[task_id]).__name__, task_type)
