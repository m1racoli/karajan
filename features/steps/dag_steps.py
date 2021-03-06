#
# Copyright 2017 Wooga GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys

from behave import *
from karajan.conductor import Conductor
from assertions import *
from config import *


def get_dag(context, dag_id):
    if '.' in dag_id:
        [dag_id, subdag_id] = dag_id.split('.')
        dag = context.dags.get(dag_id).get_task(subdag_id).subdag
    else:
        dag = context.dags.get(dag_id)
    if dag:
        return dag
    raise Exception('DAG %s not found' % dag_id)


def get_task(context, dag_id, task_id):
    return get_dag(context, dag_id).get_task(task_id)


@when(u'I build the DAGs')
def step_impl(context):
    context.dags = Conductor(get_conf(context)).build('test')


@when(u'I try to build the DAGs')
def step_impl(context):
    try:
        context.dags = Conductor(get_conf(context)).build('test')
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


@then(u'in the DAG {dag_id} {downstream_id} should depend on {upstream_id}')
def step_impl(context, dag_id, downstream_id, upstream_id):
    downstream = get_task(context, dag_id, downstream_id)
    upstream = get_task(context, dag_id, upstream_id)
    assert_contains(downstream.upstream_list, upstream)


@then(u'in the DAG {dag_id} {downstream_id} should not depend on {upstream_id}')
def step_impl(context, dag_id, downstream_id, upstream_id):
    downstream = get_task(context, dag_id, downstream_id)
    upstream = get_task(context, dag_id, upstream_id)
    assert_contains_not(downstream.upstream_list, upstream)
