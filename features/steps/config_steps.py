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

from behave import *

from config import *


@given(u'a minimal config')
def step_impl(context):
    context.conf = min_config()


@given(u'the attribute {attr} for the {model} {id} is not set')
def step_impl(context, attr, model, id):
    conf = get_model_conf(context, model).get(id)
    conf.pop(attr, None)


@given(u'the context has the following items')
def step_impl(context):
    conf = get_context_conf(context)
    conf['items'] = conf.get('items', {})
    for row in context.table:
        conf['items'][row['item']] = {k: row[k] for k in row.headings if k != 'item'}
    conf['item_column'] = 'item'


@given(u'the context has the following defaults')
def step_impl(context):
    conf = get_context_conf(context)
    conf['defaults'] = conf.get('defaults', {})
    row = context.table[0]
    for h in row.headings:
        conf['defaults'][h] = row[h]


@given(u'the {model} {id} has the following items')
def step_impl(context, model, id):
    conf = get_model_conf(context, model).get(id)
    conf['items'] = conf.get('items', [])
    for row in context.table:
        conf['items'].append(row['item'])


@given(u'the {model} {id} has the items of the context')
def step_impl(context, model, id):
    context_conf = get_context_conf(context)
    conf = get_model_conf(context, model).get(id)
    conf['items'] = list(context_conf.get('items', {}).keys())


@given(u'the {model} {id} has wildcard items')
def step_impl(context, model, id):
    conf = get_model_conf(context, model).get(id)
    conf['items'] = '*'


@given(u'the aggregation {id} is parameterized')
def step_impl(context, id):
    conf = get_aggregation_conf(context).get(id)
    conf['query'] = conf.get('query').replace('DUAL', '{{ item }}')


@given(u'the aggregation {id} has a {dep_type} dependency')
def step_impl(context, id, dep_type):
    conf = get_aggregation_conf(context).get(id)
    conf['dependencies'] = conf.get('dependencies', [])
    conf['dependencies'].append(min_dependency_config(dep_type))


@given(u'the aggregation {id} has a {dep_type} dependency with the following attributes')
def step_impl(context, id, dep_type):
    conf = get_aggregation_conf(context).get(id)
    conf['dependencies'] = conf.get('dependencies', [])
    dep_conf = min_dependency_config(dep_type)
    row = context.table[0]
    for h in row.headings:
        if h == 'columns':
            dep_conf[h] = row[h].split(',')
        else:
            dep_conf[h] = row[h]

    conf['dependencies'].append(dep_conf)


@given(u'the target {id} has a timeseries key {timeseries_key}')
def step_impl(context, id, timeseries_key):
    conf = get_target_conf(context).get(id)
    conf['key_columns'] = conf.get('key_columns', {})
    conf['timeseries_key'] = timeseries_key


@given(u'the target {id} has the following parameter columns')
def step_impl(context, id):
    conf = get_target_conf(context).get(id)
    conf['parameter_columns'] = conf.get('parameter_columns', {})
    row = context.table[0]
    for h in row.headings:
        conf['parameter_columns'][h] = row[h]


@given(u'the target {target_id} with the aggregation {aggregation_id}')
def step_impl(context, target_id, aggregation_id):
    conf = get_target_conf(context).get(target_id, {})
    if not conf:
        get_target_conf(context)[target_id] = conf
        conf['start_date'] = datetime.now()
        conf['schema'] = 'test'
        conf['key_columns'] = ['key_column']
    agg_conf = conf.get('aggregated_columns', {})
    if not agg_conf:
        conf['aggregated_columns'] = agg_conf
    agg_conf[aggregation_id] = {'%s_column' % aggregation_id: None}
    get_aggregation_conf(context)[aggregation_id] = {'query': 'SELECT * FROM DUAL', 'time_key': 'time_key'}
