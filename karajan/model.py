import re
from datetime import datetime, timedelta, date

from validations import *


class ModelBase(object):
    def __init__(self, name):
        self.name = name
        self.validate()

    def validate(self):
        validate_presence(self, 'name')


class Table(ModelBase):
    def __init__(self, name, conf):
        self.schema = conf.get('schema', None)
        super(Table, self).__init__(name)

    def validate(self):
        validate_presence(self, 'schema')
        validate_not_empty(self, 'key_columns')
        validate_not_empty(self, 'aggregated_columns')
        super(Table, self).validate()


class AggregatedTable(Table):
    def __init__(self, name, conf):
        self.start_date = self._date_time(conf.get('start_date'))
        self.key_columns = {n: Column(n, c) for n, c in conf.get('key_columns', {}).iteritems()}
        self.aggregated_columns = conf.get('aggregated_columns', {})
        self.items = conf.get('items', [{}])
        self.defaults = conf.get('defaults', {})
        self.item_key = conf.get('item_key')
        self.timeseries_key = conf.get('timeseries_key')
        super(AggregatedTable, self).__init__(name, conf)

    def dag_id(self, prefix=None):
        if not prefix:
            return self.name
        else:
            return '%s_%s' % (prefix, self.name)

    def validate(self):
        validate_presence(self, 'start_date')
        super(AggregatedTable, self).validate()

    @staticmethod
    def _date_time(o):
        if isinstance(o, date):
            return datetime(o.year, o.month, o.day)
        return o

    def param_set(self):
        l = []
        for item in self.items:
            params = {}
            params.update(self.defaults)
            params.update(item)
            l.append(params)
        return l

    def key_items(self):
        return [i[self.item_key] for i in self.items]


class Column(ModelBase):
    def __init__(self, name, conf):
        if type(conf) == str:
            self.column_type = conf
        else:
            self.column_type = conf.get('column_type')
        super(Column, self).__init__(name)

    def validate(self):
        validate_presence(self, 'column_type')
        super(Column, self).validate()


class AggregatedColumn(Column):
    def __init__(self, name, conf, table):
        self.query = conf.get('query', '')
        self.dependencies = conf.get('dependencies')
        self.parameterize = self._check_parameterize(table)
        self.column_name = name
        super(AggregatedColumn, self).__init__(name, conf)

    def validate(self):
        validate_presence(self, 'query')
        super(AggregatedColumn, self).validate()

    def _check_parameterize(self, table):
        query = self.query.replace('\n',' ') # wildcard doesn't match linebreaks
        if self._param_regex(table.item_key).match(query):
            return True
        return False

    @staticmethod
    def _param_regex(name):
        return re.compile('^.*{{ *%s *}}.*$' % name)

    def id(self):
        return ("aggregate_%s" % self.name).lower()

    def has_dependencies(self):
        return self.dependencies is not None


class BaseDependency(ModelBase):
    def id(self):
        return ("wait_for_%s" % self.name).lower()


class NothingDependency(BaseDependency):
    def __init__(self):
        super(NothingDependency, self).__init__('nothing')


class TrackingDependency(BaseDependency):
    def __init__(self, conf):
        self.schema = conf.get('schema')
        self.table = conf.get('table')
        name = ("%s_%s" % (self.schema, self.table)).lower()
        super(TrackingDependency, self).__init__(name)


class DeltaDependency(BaseDependency):
    def __init__(self, conf):
        td = conf.get('delta')
        if isinstance(td, int):
            td = timedelta(seconds=td)
        elif isinstance(td, unicode):
            td = self._parse_timedelta(td)

        self.delta = td
        name = ("%s_seconds_delta" % int(self.delta.total_seconds())).lower()
        super(DeltaDependency, self).__init__(name)

    __timedelta_regex = re.compile(
        r'((?P<weeks>\d+?)w)?((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')

    def _parse_timedelta(self, s):
        parts = self.__timedelta_regex.match(s)
        if not parts:
            return timedelta()
        parts = parts.groupdict()
        time_params = {}
        for (name, param) in parts.iteritems():
            if param:
                time_params[name] = int(param)
        return timedelta(**time_params)


class TaskDependency(BaseDependency):
    def __init__(self, conf):
        self.dag_id = conf.get('dag_id')
        self.task_id = conf.get('task_id')
        name = ("task_%s_%s" % (self.dag_id, self.task_id)).lower()
        super(TaskDependency, self).__init__(name)


d_map = {
    'tracking': TrackingDependency,
    'delta': DeltaDependency,
    'task': TaskDependency,
}


def get_dependency(conf):
    return d_map[conf.get('type')](conf)
