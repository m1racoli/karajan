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

from datetime import timedelta
from karajan.model import ModelBase


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
        else:
            td = self._parse_timedelta(td)

        self.delta = td
        name = ("%s_seconds_delta" % int(self.delta.total_seconds())).lower()
        super(DeltaDependency, self).__init__(name)

    __timedelta_regex = re.compile(
        r'((?P<weeks>\d+?)w)?((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')

    def _parse_timedelta(self, s):
        parts = self.__timedelta_regex.match(s)
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


class TargetDependency(BaseDependency):
    def __init__(self, conf):
        self.target = conf.get('target')
        self.columns = conf.get('columns')
        name = ("target_%s" % self.target).lower()
        super(TargetDependency, self).__init__(name)


d_map = {
    'tracking': TrackingDependency,
    'delta': DeltaDependency,
    'task': TaskDependency,
    'target': TargetDependency,
}


def get_dependency(conf):
    return d_map[conf.get('type')](conf)
