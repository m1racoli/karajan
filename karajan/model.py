import re
from datetime import datetime, date

from validations import *


class ModelBase(object, Validatable):
    def __init__(self, name):
        self.name = name
        self.validate()
        super(ModelBase, self).__init__()

    def validate(self):
        self.validate_presence('name')


class Context(ModelBase):
    def __init__(self, conf):
        self.items = conf.get('items', {})
        for k, v in self.items.iteritems():
            if not v:
                self.items[k] = {}
        self.defaults = conf.get('defaults', {})
        self.item_column = conf.get('item_column', None)
        super(Context, self).__init__('context')

    def validate(self):
        if self.is_parameterized():
            self.validate_presence('item_column')
            for k, v in self.items.iteritems():
                validate_exclude(v, 'item')
        else:
            self.validate_absence('item_column')
        self.validate_exclude('defaults', 'item')
        super(Context, self).validate()

    def is_parameterized(self):
        return len(self.items) > 0

    def item_keys(self):
        if self.is_parameterized():
            # give me all distinct keys (k) for each item's parameter dict (d)
            keys = {k for d in self.items.values() for k in d}
            keys.add('item')
            keys.update(self.defaults.keys())
            return keys
        else:
            return self.defaults.keys()

    def params(self, target=None):
        if self.is_parameterized():
            def make_params(item):
                params = {}
                params.update(self.defaults)
                params.update(self.items[item])
                params['item'] = item
                return params

            return {k: make_params(k) for k in (target.items if target else self.items)}
        else:
            return {'': self.defaults}


class Target(ModelBase):
    def __init__(self, name, conf, context):
        self.context = context
        self.schema = conf.get('schema', None)
        self.start_date = self._date_time(conf.get('start_date'))
        self.key_columns = conf.get('key_columns', [])
        if context.is_parameterized():
            self.key_columns.append(context.item_column)
        self.aggregations = \
            {agg_id: {cname: AggregatedColumn(agg_id, cname, conf) for cname, conf in agg_columns.iteritems()}
             for agg_id, agg_columns in
             conf.get('aggregated_columns', {}).iteritems()}
        self.items = conf.get('items', [])
        if self.items == '*':
            self.items = self.context.items.keys()
        self.timeseries_key = conf.get('timeseries_key')
        self.parameter_columns = conf.get('parameter_columns', {})
        super(Target, self).__init__(name)

    def has_item(self, item):
        return not item or item in self.items

    def validate(self):
        self.validate_presence('schema')
        self.validate_presence('start_date')
        self.validate_not_empty('key_columns')
        self.validate_not_empty('aggregations', 'aggregated_columns')
        if self.context.is_parameterized():
            self.validate_not_empty('items')
            for i in self.items:
                validate_include(self.context.items, i)
        else:
            self.validate_empty('items')
        if self.timeseries_key:
            self.validate_in('timeseries_key', self.key_columns)
        for item_key in self.parameter_columns.values():
            validate_include(self.context.item_keys(), item_key)

        super(Target, self).validate()

    @staticmethod
    def _date_time(o):
        if isinstance(o, date):
            return datetime(o.year, o.month, o.day)
        return o

    def aggregated_columns(self, aggregation_id=None):
        if aggregation_id:
            return self.aggregations.get(aggregation_id)
        return {n: ac for v in self.aggregations.values() for n, ac in v.iteritems()}

    def is_timeseries(self):
        return self.timeseries_key is not None

    def has_parameter_columns(self):
        return True if self.parameter_columns else False

    def src_column_names(self, aggregation_id):
        if not self.aggregations.get(aggregation_id):
            return []
        return self.key_columns + [ac.src_column_name for ac in self.aggregations.get(aggregation_id, {}).values()]

    def depends_on_past(self, aggregation_id):
        return not self.is_timeseries() and any(
            ac.depends_on_past() for ac in self.aggregations[aggregation_id].values())

    def table(self):
        return "%s.%s" % (self.schema, self.name)

    def aggregations_for_columns(self, columns):
        if not columns:
            return self.aggregations

        def cols_in(v):
            for c in columns:
                if c in v:
                    return True
            return False

        return {k: v for k, v in self.aggregations.iteritems() if cols_in(v)}


class AggregatedColumn(ModelBase):
    replace_update_type = 'REPLACE'
    keep_update_type = 'KEEP'
    min_update_type = 'MIN'
    max_update_type = 'MAX'
    _default_update_type = replace_update_type
    _update_types = {replace_update_type, keep_update_type, min_update_type, max_update_type}
    _depends_on_past_update_types = {replace_update_type, keep_update_type}

    def __init__(self, aggregation_id, column_name, conf):
        self.aggregation_id = aggregation_id
        self.column_name = column_name
        if conf is None:
            self.src_column_name = self.column_name
            self.update_type = self._default_update_type
        elif isinstance(conf, str):
            self.src_column_name = conf
            self.update_type = self._default_update_type
        else:
            self.src_column_name = conf.get('column_name', self.column_name)
            self.update_type = conf.get('update_type', self._default_update_type).upper()
        super(AggregatedColumn, self).__init__(column_name)

    def validate(self):
        self.validate_presence('aggregation_id')
        self.validate_presence('column_name')
        self.validate_presence('src_column_name')
        self.validate_in('update_type', self._update_types)
        super(AggregatedColumn, self).validate()

    def depends_on_past(self):
        return self.update_type in self._depends_on_past_update_types


class Aggregation(ModelBase):
    def __init__(self, name, conf, context):
        self.context = context
        self.query = conf.get('query', '')
        self.dependencies = conf.get('dependencies')
        self.offset = conf.get('offset', 0)
        self.parameterize = self._check_parameterize()
        super(Aggregation, self).__init__(name)

    def validate(self):
        self.validate_presence('query')
        super(Aggregation, self).validate()

    def _check_parameterize(self):
        if not self.context.is_parameterized():
            return False
        query = self.query.replace('\n', ' ')  # wildcard doesn't match linebreaks
        if self._param_regex('item').match(query):  # might change in the future
            return True
        # for k in self.context.item_keys():
        #     if self._param_regex(k).match(query):
        #         return True
        return False

    @staticmethod
    def _param_regex(name):
        return re.compile('^.*{{ *%s *}}.*$' % name)

    def has_dependencies(self):
        return self.dependencies is not None
