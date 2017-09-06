from karajan.validations import Validatable


class BaseTransformation(object, Validatable):
    def __init__(self, conf):
        self.columns = conf.get('columns')
        self.validate()
        super(BaseTransformation, self).__init__()

    def validate(self):
        self.validate_presence('columns')

    def transform(self, tmp_table):
        raise NotImplementedError()


class UpdateTransformation(BaseTransformation):
    def __init__(self, conf):
        self.query = conf.get('query')
        super(UpdateTransformation, self).__init__(conf)

    def validate(self):
        self.validate_presence('query')

    def transform(self, tmp_table):
        return "UPDATE {tmp_table}\n{query}".format(
            tmp_table=tmp_table,
            query=self.query
        )


t_map = {
    'update': UpdateTransformation,
}


def get(conf):
    return t_map[conf.get('type')](conf)
