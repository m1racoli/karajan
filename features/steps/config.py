from datetime import datetime


def get_conf(context):
    if 'conf' not in context:
        context.conf = {
            'targets': {},
            'aggregations': {},
        }
    return context.conf


def get_model_conf(context, model):
    conf = get_conf(context)
    return conf.get('%ss' % model)


def get_aggregation_conf(context):
    return get_model_conf(context, 'aggregation')


def get_target_conf(context):
    return get_model_conf(context, 'target')


def min_config():
    return {
        'targets': {
            "test": {
                'start_date': datetime.now(),
                'schema': 'test',
                'key_columns': {
                    'key_column': 'VARCHAR(100)',
                },
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
    }


def min_dependency_config(dep_type):
    conf = {
        'tracking': {'schema': 'test', 'table': 'test'},
        'delta': {'delta': 0}
    }[dep_type]
    conf['type'] = dep_type
    return conf
