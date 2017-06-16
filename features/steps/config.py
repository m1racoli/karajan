from datetime import datetime


def get_conf(context):
    if 'conf' not in context:
        context.conf = {
            'tables': {},
            'columns': {},
        }
    return context.conf


def min_config():
    return {
        'tables': {
            "test": {
                'start_date': datetime.now(),
                'schema': 'test',
                'key_columns': {
                    'key_column': 'VARCHAR(100)',
                },
                'aggregated_columns': {
                    'test': 'test',
                }
            }
        },
        'columns': {
            'test': {
                'query': "SELECT 'key' AS key_column, 'val' AS val FROM DUAL",
                'column_type': 'VARCHAR(100)',
            }
        },
    }
