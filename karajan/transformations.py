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
