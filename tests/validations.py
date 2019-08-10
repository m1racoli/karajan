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

from unittest import TestCase

from nose.tools import assert_equal
from parameterized import parameterized

from karajan.validations import *


def _validate(fun, expected, *args):
    valid = True
    try:
        fun(*args)
    except KarajanValidationException:
        valid = False
    assert_equal(valid, expected)


@parameterized([
    (None, False),
    ('', False),
    ('0', True),
    ('1', True),
    ('x', True),
    (0, True),
    (1, True),
    (True, True),
    (False, True),
    ({}, True),
    ([], True),
    ({'a': 'b'}, True),
    (['c'], True),
])
def test_validate_presence(val, expected):
    _validate(validate_presence, expected, val)


@parameterized([
    (None, True),
    ('', True),
    ('0', False),
    ('1', False),
    ('x', False),
    (0, False),
    (1, False),
    (True, False),
    (False, False),
    ({}, False),
    ([], False),
    ({'a': 'b'}, False),
    (['c'], False),
])
def test_validate_absense(val, expected):
    _validate(validate_absence, expected, val)


@parameterized([
    (None, False),
    ('', False),
    ('0', False),
    ('1', False),
    ('x', False),
    (0, False),
    (1, False),
    (True, False),
    (False, False),
    ({}, True),
    ([], True),
    ({'a': 'b'}, False),
    (['c'], False),
])
def test_validate_empty(val, expected):
    _validate(validate_empty, expected, val)


@parameterized([
    (None, False),
    ('', False),
    ('0', False),
    ('1', False),
    ('x', False),
    (0, False),
    (1, False),
    (True, False),
    (False, False),
    ({}, False),
    ([], False),
    ({'a': 'b'}, True),
    (['c'], True),
])
def test_validate_not_empty(val, expected):
    _validate(validate_not_empty, expected, val)


@parameterized([
    ([], None, False),
    ([], 'a', False),
    (['c'], None, False),
    (['c'], 'a', False),
    ([None], None, True),
    (['a'], 'a', True),
    ({}, None, False),
    ({}, 'a', False),
    ({'c': 'd'}, None, False),
    ({'c': 'd'}, 'a', False),
    ({None: 'd'}, None, True),
    ({'a': 'd'}, 'a', True),
])
def test_validate_include(items, val, expected):
    _validate(validate_include, expected, items, val)


@parameterized([
    ([], None, True),
    ([], 'a', True),
    (['c'], None, True),
    (['c'], 'a', True),
    ([None], None, False),
    (['a'], 'a', False),
    ({}, None, True),
    ({}, 'a', True),
    ({'c': 'd'}, None, True),
    ({'c': 'd'}, 'a', True),
    ({None: 'd'}, None, False),
    ({'a': 'd'}, 'a', False),
])
def test_validate_exclude(items, val, expected):
    _validate(validate_exclude, expected, items, val)


class TestValidatable(TestCase):
    def setUp(self):
        self.validatable = Validatable()

    def test_validate(self):
        self.validatable.validate()

    def _validate(self, fun, expected, val, *args):
        valid = True
        setattr(self.validatable, 'attribute', val)
        try:
            fun('attribute', *args)
        except KarajanValidationException:
            valid = False
        assert_equal(valid, expected)

    @parameterized.expand([
        ('none', None, False),
        ('empty_str', '', False),
        ('0_str', '0', True),
        ('1_str', '1', True),
        ('x', 'x', True),
        ('0', 0, True),
        ('1', 1, True),
        ('true', True, True),
        ('false', False, True),
        ('dict', {}, True),
        ('list', [], True),
    ])
    def test_validate_presence(self, _, val, expected):
        self._validate(self.validatable.validate_presence, expected, val)

    @parameterized.expand([
        ('none', None, True),
        ('empty_str', '', True),
        ('0_str', '0', False),
        ('1_str', '1', False),
        ('x', 'x', False),
        ('0', 0, False),
        ('1', 1, False),
        ('true', True, False),
        ('false', False, False),
        ('dict', {}, False),
        ('list', [], False),
    ])
    def test_validate_absence(self, _, val, expected):
        self._validate(self.validatable.validate_absence, expected, val)

    @parameterized.expand([
        ('none', None, False),
        ('empty_str', '', False),
        ('0_str', '0', False),
        ('1_str', '1', False),
        ('x', 'x', False),
        ('0', 0, False),
        ('1', 1, False),
        ('true', True, False),
        ('false', False, False),
        ('dict', {}, True),
        ('list', [], True),
        ('dict', {'a': 'b'}, False),
        ('list', ['c'], False),
    ])
    def test_validate_empty(self, _, val, expected):
        self._validate(self.validatable.validate_empty, expected, val)


    @parameterized.expand([
        ('none', None, False),
        ('empty_str', '', False),
        ('0_str', '0', False),
        ('1_str', '1', False),
        ('x', 'x', False),
        ('0', 0, False),
        ('1', 1, False),
        ('true', True, False),
        ('false', False, False),
        ('dict', {}, False),
        ('list', [], False),
        ('dict', {'a': 'b'}, True),
        ('list', ['c'], True),
    ])
    def test_validate_not_empty(self, _, val, expected):
        self._validate(self.validatable.validate_not_empty, expected, val)

    @parameterized.expand([
        ([], None, False),
        ([], 'a', False),
        (['c'], None, False),
        (['c'], 'a', False),
        ([None], None, True),
        (['a'], 'a', True),
        ({}, None, False),
        ({}, 'a', False),
        ({'c': 'd'}, None, False),
        ({'c': 'd'}, 'a', False),
        ({None: 'd'}, None, True),
        ({'a': 'd'}, 'a', True),
    ])
    def test_validate_include(self, items, val, expected):
        self._validate(self.validatable.validate_include, expected, items, val)

    @parameterized.expand([
        ([], None, True),
        ([], 'a', True),
        (['c'], None, True),
        (['c'], 'a', True),
        ([None], None, False),
        (['a'], 'a', False),
        ({}, None, True),
        ({}, 'a', True),
        ({'c': 'd'}, None, True),
        ({'c': 'd'}, 'a', True),
        ({None: 'd'}, None, False),
        ({'a': 'd'}, 'a', False),
    ])
    def test_validate_exclude(self, items, val, expected):
        self._validate(self.validatable.validate_exclude, expected, items, val)

    @parameterized.expand([
        ([], None, False),
        ([], 'a', False),
        (['c'], None, False),
        (['c'], 'a', False),
        ([None], None, True),
        (['a'], 'a', True),
        ({}, None, False),
        ({}, 'a', False),
        ({'c': 'd'}, None, False),
        ({'c': 'd'}, 'a', False),
        ({None: 'd'}, None, True),
        ({'a': 'd'}, 'a', True),
    ])
    def test_validate_in(self, items, val, expected):
        self._validate(self.validatable.validate_in, expected, val, items)

    @parameterized.expand([
        ([], None, True),
        ([], 'a', True),
        (['c'], None, True),
        (['c'], 'a', True),
        ([None], None, False),
        (['a'], 'a', False),
        ({}, None, True),
        ({}, 'a', True),
        ({'c': 'd'}, None, True),
        ({'c': 'd'}, 'a', True),
        ({None: 'd'}, None, False),
        ({'a': 'd'}, 'a', False),
    ])
    def test_validate_not_in(self, items, val, expected):
        self._validate(self.validatable.validate_not_in, expected, val, items)

    def test_validate_conf_name(self):
        ex = None
        setattr(self.validatable, 'attribute', '')
        try:
            self.validatable.validate_presence('attribute', 'conf_name')
        except KarajanValidationException as e:
            ex = e
        assert isinstance(ex, KarajanValidationException)
        assert_equal(ex.args[0], 'Validatable: conf_name must be present')
