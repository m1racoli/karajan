from nose.tools import assert_in

from karajan.conductor import Conductor
from tests.helpers.config import ConfigHelper


def test_import_subdags():
    conf = ConfigHelper().parameterize_context()
    dags = Conductor(conf).build('test_dag', import_subdags=True)
    assert_in('test_dag', dags)
    assert_in('test_dag.item', dags)
