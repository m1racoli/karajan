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

Feature: Validation
  Validate the configuration

  Background:
    Given a minimal config

  Scenario Outline: Required target attributes
    Given the attribute <attr> for the target test is not set
    When I try to build the DAGs
    Then there should be no DAGs
    Then there should have been an exception KarajanValidationException

    Examples:
      | attr               |
      | start_date         |
      | schema             |
      | key_columns        |
      | aggregated_columns |

  Scenario Outline: Required aggregation attributes
    Given the attribute <attr> for the aggregation test_agg is not set
    When I try to build the DAGs
    Then there should be no DAGs
    Then there should have been an exception KarajanValidationException

    Examples:
      | attr     |
      | query    |
      | time_key |
