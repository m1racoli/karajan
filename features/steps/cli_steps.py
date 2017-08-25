from airflow.bin.cli import CLIFactory
from behave import *

from karajan.bin.cli import KarajanCLIFactory
from tests.helpers import defaults


@when(u'I trigger a Karajan run')
def step_impl(context):
    exec_karajan('run', defaults.KARAJAN_ID)


@when(u'I trigger a Karajan run for a target column')
def step_impl(context):
    exec_karajan('run', '-l', 'test_table[test_column]', defaults.KARAJAN_ID)


@when(u'I run "airflow {args}"')
def step_impl(context, args):
    exec_airflow(*args.split(' '))


@when(u'I run "karajan {args}"')
def step_impl(context, args):
    exec_karajan(*args.split(' '))


def exec_airflow(*a):
    args = CLIFactory.get_parser().parse_args(a)
    args.func(args)


def exec_karajan(*a):
    args = KarajanCLIFactory.get_parser().parse_args(a)
    args.func(args)
