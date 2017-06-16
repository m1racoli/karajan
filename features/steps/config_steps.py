from behave import *

from config import *


@given(u'a minimal config')
def step_impl(context):
    context.conf = min_config()


@given(u'the attribute {attr} for the {model} {table} is not set')
def step_impl(context, attr, model, table):
    conf = get_conf(context)
    table_conf = conf.get('%ss' % model).get(table)
    table_conf.pop(attr, None)
