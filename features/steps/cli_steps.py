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
