from airflow import configuration as airflow_conf
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
            dags_path = path.expanduser(airflow_conf.get('core', 'DAGS_FOLDER'))
            conf = path.join(dags_path, conf)

        return {
            'targets': Config.__load(path.join(conf, 'targets.yml')),
            'aggregations': Config.__load(path.join(conf, 'aggregations.yml')),
            'context': Config.__load(path.join(conf, 'context.yml')),
        }

    template_ignore_keywords = ['ds']
    template_ignore_mapping = {k: '{{ %s }}' % k for k in template_ignore_keywords}

    @classmethod
    def render(cls, conf, params, replace=None):
        """

        :param conf: the conf object to apply jinja2 templating
        :type conf: dict, list, str, unicode
        :param params: the params to use in the rendering. ignored keys will be overwritten
        :type params: dict
        :param replace: instead if rendering, replace {{ k }} with {{ v }} for each k,v in replace.
        will be applied on params and ignored keywords
        :type replace: dict
        :return:
        """
        if replace is None:
            replace = {}
        else:
            replace = {k: '{{ %s }}' % v for k, v in replace.iteritems()}

        if isinstance(conf, dict):
            return {k: cls.render(v, params) for k, v in conf.iteritems()}
        elif isinstance(conf, list):
            return [cls.render(v, params) for v in conf]
        elif isinstance(conf, (str, unicode)):
            render_params = dict()
            render_params.update(params)
            render_params.update(cls.template_ignore_mapping)
            render_params.update(replace)
            return Template(conf).render(**render_params)
        return conf
