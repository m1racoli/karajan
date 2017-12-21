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

from karajan.config import Config
from karajan.validations import Validatable


class BaseTransformation(object, Validatable):
    def __init__(self, conf):
        self.columns = conf.get('columns')
        self.validate()
        super(BaseTransformation, self).__init__()

    def validate(self):
        self.validate_presence('columns')

    def transform(self, tmp_table, params):
        raise NotImplementedError()

    def applies_to(self, columns):
        if any(c in self.columns for c in columns):
            return True
        return False


class UpdateTransformation(BaseTransformation):
    def __init__(self, conf):
        self.query = conf.get('query')
        super(UpdateTransformation, self).__init__(conf)

    def validate(self):
        self.validate_presence('query')

    def transform(self, tmp_table, params):
        sql = "UPDATE {tmp_table}\n{query}".format(
            tmp_table=tmp_table,
            query=self.query
        )
        return Config.render(sql, params)


class MergeTransformation(BaseTransformation):
    def __init__(self, conf):
        self.query = conf.get('query')
        super(MergeTransformation, self).__init__(conf)

    def validate(self):
        self.validate_presence('query')

    def transform(self, tmp_table, params):
        sql = "MERGE INTO {tmp_table} tmp\n{query}".format(
            tmp_table=tmp_table,
            query=self.query
        )
        return Config.render(sql, params)


t_map = {
    'update': UpdateTransformation,
    'merge': MergeTransformation,
}


def get(conf):
    return t_map[conf.get('type')](conf)
