import airflow.configuration
import yaml
from os import path


class Config(object):
    @staticmethod
    def __load(file_path):
        return yaml.load(file(file_path, 'r'))

    @classmethod
    def load(cls, conf):
        if isinstance(conf, dict):
            return conf

        if not path.isabs(conf):
            dags_path = airflow.configuration.get_dags_folder()
            conf = path.join(dags_path, conf)

        return {
            'tables': Config.__load(path.join(conf, 'tables.yml')),
            'columns': Config.__load(path.join(conf, 'columns.yml'))
        }
