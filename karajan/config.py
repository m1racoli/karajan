import airflow.configuration
import yaml
from os import path
from jinja2 import Template


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
            'targets': Config.__load(path.join(conf, 'targets.yml')),
            'aggregations': Config.__load(path.join(conf, 'aggregations.yml')),
            'context': Config.__load(path.join(conf, 'context.yml')),
        }

    template_ignore_keywords = ['ds']
    template_ignore_mapping = {k: '{{ %s }}' % k for k in template_ignore_keywords}

    @classmethod
    def render(cls, conf, params):
        if isinstance(conf, dict):
            return {k: cls.render(v, params) for k, v in conf.iteritems()}
        elif isinstance(conf, list):
            return [cls.render(v, params) for v in conf]
        elif isinstance(conf, str):
            render_params = dict()
            render_params.update(params)
            render_params.update(cls.template_ignore_mapping)
            return Template(conf).render(**render_params)
        return conf
