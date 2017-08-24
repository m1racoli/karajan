from airflow.models import DagRun
from airflow.utils.state import State
from behave import *

from tests.helpers import defaults


@then(u'there should be an active DAG run')
def step_impl(context):
    assert len(get_dag_runs(context)) == 1


@then(u'there should be no active DAG runs')
def step_impl(context):
    assert len(get_dag_runs(context)) == 0


@then(u'the DAG run should be limited to the target column and it\'s source column')
def step_impl(context):
    dr = get_dag_runs(context)[0]
    limit = dr.conf['limit']
    assert limit, "message no limit specified for {} with conf {}".format(dr, dr.conf)
    assert limit['test_table']
    assert 'test_column' in limit['test_table']
    assert limit['test_aggregation']
    for src_column in ['key_column', 'test_src_column', 'test_time_key']:
        assert src_column in limit['test_aggregation']


def get_dag_runs(context, dag_id=defaults.KARAJAN_ID, state=State.RUNNING, external_trigger=True):
    if hasattr(context, 'dag_runs'):
        return context.dag_runs
    drs = DagRun.find(
        dag_id=dag_id,
        state=state,
        external_trigger=external_trigger,
    )
    setattr(context, 'dag_runs', drs)
    return drs
