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

import logging
from datetime import datetime, date, timedelta

import airflow.bin.cli as cli
from airflow import settings
from airflow.models import DagBag
from airflow.utils.state import State
from dateutil.parser import parse as parsedate

from karajan.exceptions import KarajanException
from karajan.model import KarajanDAG, LimitFilter


def run(args):
    # load DAGs and find KarajanDAGs
    dags = get_dags(args)

    run_id = "karajan_run_{}".format(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
    start_date = args.start_date if args.start_date else yesterday()
    end_date = args.end_date if args.end_date else yesterday()

    items = args.items
    if items:
        items = set(items.split(','))

    # create DAG runs
    for dag in dags.values():
        if items and dag.item not in items:
            continue

        conf = {
            'start_date': start_date,
            'end_date': end_date,
        }

        if args.limit:
            conf['limit'] = dag.limit(args.limit)

        logging.info("trigger {} ({}) from {} to {}".format(dag.dag_id, run_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        dag.create_dagrun(
            run_id=run_id,
            state=State.RUNNING,
            conf=conf,
            external_trigger=True
        )


def yesterday():
    return datetime.combine(date.today(), datetime.min.time()) - timedelta(days=1)


def get_dags(args):
    dagbag = DagBag(cli.process_subdir(args.subdir))
    dags = {id: dag for id, dag in dagbag.dags.iteritems() if isinstance(dag, KarajanDAG)}
    # check weither we want selected KarajanDAGs or all of them
    if hasattr(args, 'karajan_id'):
        dags = {id: dag for id, dag in dags.iteritems() if dag.karajan_id == args.karajan_id}
        if not dags:
            raise KarajanException('no DAGs with karajan_id {} found.'.format(args.karajan_id))
        return dags
    return dags


class KarajanCLIFactory(cli.CLIFactory):
    args = {
        'karajan_id': cli.Arg(("karajan_id",), "The id of the Karajan setup"),
        'subdir': cli.Arg(
            ("-sd", "--subdir"),
            "File location or directory from which to look for the dag",
            default=settings.DAGS_FOLDER),
        'start_date': cli.Arg(
            ("-s", "--start_date"), "Set start_date YYYY-MM-DD",
            type=parsedate),
        'end_date': cli.Arg(
            ("-e", "--end_date"), "Set end_date YYYY-MM-DD",
            type=parsedate),
        'items': cli.Arg(
            ("-i", "--items"), "Run for selected items"),
        'limit': cli.Arg(
            ("-l", "--limit"), "Limit calculations to a subset", type=LimitFilter.parse),
    }

    subparsers = (
        {
            'func': run,
            'help': "Trigger aggregations for a Karajan setup",
            'args': ('karajan_id', 'subdir', 'start_date', 'end_date', 'items', 'limit'),
        },
    )
    subparsers_dict = {sp['func'].__name__: sp for sp in subparsers}
