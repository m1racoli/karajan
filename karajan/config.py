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

    # currently we dont have airflow keywords we want to ingore. will if we have use for it later
    # template_ignore_keywords = ['ds']
    # template_ignore_mapping = {k: '{{ %s }}' % k for k in template_ignore_keywords}

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
            # render_params.update(cls.template_ignore_mapping)
            render_params.update(replace)
            return Template(conf).render(**render_params)
        return conf
