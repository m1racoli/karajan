import os
import logging

# first set AIRFLOW_HOME before loading any airflow modules
os.environ["AIRFLOW_HOME"] = "."
from airflow.utils import db as db_utils


def before_all(context):
    # we don't want to see all the migration steps during the tests
    logging.getLogger("alembic.runtime.migration").disabled = True
    db_utils.initdb()


def before_tag(context, tag):
    if tag == 'db':
        # make sure the test db is clean
        db_utils.resetdb()


def after_all(context):
    os.remove('./test.db')
