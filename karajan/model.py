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

import re
from argparse import ArgumentTypeError
from datetime import datetime, date

from airflow.models import DAG

from karajan import transformations as tf
from karajan.exceptions import KarajanException
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
                validate_exclude(v, 'item_column')
        else:
            self.validate_absence('item_column')
        self.validate_exclude('defaults', 'item')
        self.validate_exclude('defaults', 'item_column')
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
                params['item_column'] = self.item_column
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
            self.validate_not_in('timeseries_key', self.key_columns)
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
    depends_on_past_update_types = {replace_update_type, keep_update_type}

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
        return self.update_type in self.depends_on_past_update_types


class Aggregation(ModelBase):
    def __init__(self, name, conf, context):
        self.context = context
        self.query = conf.get('query', '')
        self.dependencies = conf.get('dependencies')
        self.offset = conf.get('offset', 0)
        self.reruns = conf.get('reruns', 0)
        self.parameterize = self._check_parameterize()
        self.time_key = conf.get('time_key')
        self.transformations = [tf.get(tc) for tc in conf.get('transformations', [])]
        super(Aggregation, self).__init__(name)

    def validate(self):
        self.validate_presence('query')
        self.validate_presence('time_key')
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

    def retrospec(self):
        return self.offset + self.reruns

    def transformation_upstream_columns(self, columns):
        result = set(columns)
        transformations = []
        for t in reversed(self.transformations):
            if t.applies_to(result):
                transformations.append(t)
                result.update(t.columns.keys())
                for v in t.columns.values():
                    result.update(v)
        return result, reversed(transformations)


class KarajanDAG(DAG):
    def __init__(self, karajan_id, engine, item, targets, aggregations, *args, **kwargs):
        self.karajan_id = karajan_id
        self.engine = engine
        self.item = item
        self.targets = targets
        self.aggregations = aggregations
        dag_id = "%s_%s" % (karajan_id, item) if item else karajan_id
        super(KarajanDAG, self).__init__(*args, dag_id=dag_id, catchup=False, **kwargs)

    def limit(self, limit_filters):
        if not limit_filters:
            return None

        if len(limit_filters) > 1:
            raise KarajanException("multiple limit clauses are currently not supported")

        filter = limit_filters[0]
        limit = {filter.name: filter.columns}
        target = self.targets.get(filter.name)
        aggregated_columns = target.aggregated_columns()
        assert target, "not target with ID {} found".format(filter.name)

        columns = filter.columns if filter.columns else aggregated_columns.keys()

        for c in columns:
            ac = aggregated_columns.get(c)
            assert ac, "column {} not found in target {}".format(c, target)

            aggregation = self.aggregations.get(ac.aggregation_id)
            assert aggregation, "aggregation {} not found".format(ac.aggregation_id)

            agg_limit = limit.get(ac.aggregation_id)
            if not agg_limit:
                agg_limit = set(target.key_columns)
                agg_limit.add(aggregation.time_key)
                if self.item_column:
                    agg_limit.add(self.item_column)
                limit[ac.aggregation_id] = agg_limit
            agg_limit.add(ac.src_column_name)

        return limit

    @property
    def item_column(self):
        return self.params.get('item_column')


class LimitFilter(object):

    _pattern = re.compile(r"^(?P<name>[^\[\]]+)(\[(?P<cols>[^\[\]]*)\])?$")

    @classmethod
    def parse(cls, s):
        """
        :param s: String to parse
        :type s: str
        """
        if not s:
            return None

        # TODO use different filter separator, since ';' doesn't really work in the command line
        if ';' in s:
            return [l for f in s.split(';') for l in cls.parse(f)]

        if '[' in s or ']' in s:
            m = cls._pattern.match(s)

            if not m:
                raise ArgumentTypeError('could not parse {}'.format(s))

            return [cls(m.group('name'), m.group('cols').split(','))]
        else:
            return [cls(s)]

    def __init__(self, name, columns=list()):
        self.name = name
        self.columns = columns

    def __repr__(self):
        return "LimitFilter({})".format(', '.join([self.name] + self.columns))
