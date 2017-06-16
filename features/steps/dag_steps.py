import sys

from behave import *
from karajan.conductor import Conductor
from assertions import *
from config import *


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


@then(u'there should have been an exception "{exception_type}"')
def step_impl(context, exception_type):
    assert_contains(context, 'exception')
    assert_equals(context.exception.get('type'), exception_type)
