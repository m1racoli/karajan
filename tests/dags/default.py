# DAG airflow -> dont remove this comment
from karajan.conductor import Conductor
from tests.helpers import defaults as defaults
from tests.helpers.config import ConfigHelper

Conductor(conf=ConfigHelper()).build(defaults.KARAJAN_ID, output=globals())
