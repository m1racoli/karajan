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

from datetime import datetime


def get_conf(context):
    if 'conf' not in context:
        context.conf = {
            'targets': {},
            'aggregations': {},
            'context': {},
        }
    return context.conf


def get_model_conf(context, model):
    conf = get_conf(context)
    return conf.get('%ss' % model)


def get_aggregation_conf(context):
    return get_model_conf(context, 'aggregation')


def get_target_conf(context):
    return get_model_conf(context, 'target')


def get_context_conf(context):
    conf = get_conf(context)
    return conf.get('context')


def min_config():
    return {
        'targets': {
            "test": {
                'start_date': datetime.now(),
                'schema': 'test',
                'key_columns': [
                    'key_column',
                ],
                'aggregated_columns': {
                    'test_agg': {
                        'test_val': None,
                    },
                }
            }
        },
        'aggregations': {
            'test_agg': {
                'query': "SELECT 'key' AS key_column, 'test_val' AS test_val FROM DUAL",
                'time_key': 'time_key'
            }
        },
        'context': {},
    }


def min_dependency_config(dep_type):
    conf = {
        'tracking': {'schema': 'test', 'table': 'test'},
        'delta': {'delta': 0},
        'task': {'dag_id': 'test', 'task_id': 'test'},
        'target': {'target': 'test'},
    }[dep_type]
    conf['type'] = dep_type
    return conf
