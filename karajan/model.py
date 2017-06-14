from airflow.models import DAG
from jinja2 import Template
from jinja2.environment import Environment

from karajan.validations import validate_presence


class ModelBase(object):
    def __init__(self, name, conf):
        self.name = name

    def validate(self):
        validate_presence(self.name)


class Table(ModelBase):
    def __init__(self, name, conf):
        self.schema = conf['schema']
        super(Table, self).__init__(name, conf)


class AggregatedTable(Table):
    def __init__(self, name, conf):
        self.start_date = conf.get('start_date')
        self.key_columns = {n: Column(n, c) for n, c in conf.get('key_columns', {}).iteritems()}
        self.aggregated_columns = conf.get('aggregated_columns', {})
        self.items = conf.get('items', [])
        self.defaults = conf.get('defaults', {})
        self.item_key = conf.get('item_key')
        super(AggregatedTable, self).__init__(name, conf)

    def dag_id(self, prefix=None):
        if prefix is None:
            return self.name
        else:
            return '%s_%s' % (prefix, self.name)


class Column(ModelBase):
    def __init__(self, name, conf):
        if type(conf) == str:
            self.column_type = conf
        else:
            self.column_type = conf['column_type']
        super(Column, self).__init__(name, conf)


class AggregatedColumn(Column):
    def __init__(self, name, conf, params):
        conf = self._render_conf(conf, params)
        self.query = conf.get('query', '')
        self.dependencies = [get_dependency(c) for c in conf.get('dependencies', [])]
        self.parameterize = conf.get('parameterize', False)
        self.column_name = name
        name = "%s_%s" % (name, params['item_key']) if self.parameterize else name
        super(AggregatedColumn, self).__init__(name, conf)

    template_ignore_keywords = ['ds']
    template_ignore_mapping = {k: '{{ %s }}' % k for k in template_ignore_keywords}

    def _render_conf(self, conf, params):
        if isinstance(conf, dict):
            return {k: self._render_conf(v, params) for k, v in conf.iteritems()}
        elif isinstance(conf, list):
            return [self._render_conf(v, params) for v in conf]
        elif isinstance(conf, str):
            return Template(conf).render(params=params, **self.template_ignore_mapping)
        return conf

    def id(self):
        return ("aggregate_%s" % self.name).lower()

    def dependency_ids(self):
        return [d.id() for d in self.dependencies]


class Dependency(ModelBase):
    def id(self):
        return ("wait_%s" % self.name).lower()


class TrackingDependency(Dependency):
    def __init__(self, conf):
        self.schema = conf.get('schema')
        self.table = conf.get('table')
        name = ("%s_%s" % (self.schema, self.table)).lower()
        super(TrackingDependency, self).__init__(name, conf)


d_map = {
    'tracking': TrackingDependency
}


def get_dependency(conf):
    type = conf.get('type')
    return d_map[type](conf)
