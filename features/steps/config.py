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
                    'test': {
                        'test_val': None,
                    },
                }
            }
        },
        'aggregations': {
            'test': {
                'query': "SELECT 'key' AS key_column, 'test_val' AS test_val FROM DUAL",
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
