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

Feature: Parameter columns

  Background:
    Given a minimal config
    And the context has the following defaults
      | platform |
      | android  |

  Scenario: No parameterization
    Given the target test has the following parameter columns
      | platform |
      | platform |
    When I build the DAGs
    Then in the DAG test finish_test should depend on merge_test_agg_test
    And in the DAG test done should depend on finish_test

  Scenario: Parameterized context
    Given the context has the following items
      | item | family |
      | g9   | PP     |
      | pl   | JP     |
    And the target test has the following parameter columns
      | game_family |
      | family      |
    And the target test has the items of the context
    When I build the DAGs
    Then in the DAG test_g9 finish_test should depend on merge_test_agg_test
    And in the DAG test_pl finish_test should depend on merge_test_agg_test
    And in the DAG test_g9 done should depend on finish_test
    And in the DAG test_pl done should depend on finish_test

  Scenario: Parameterized context and aggregation
    Given the context has the following items
      | item | family |
      | g9   | PP     |
      | pl   | JP     |
    And the target test has the following parameter columns
      | game_family |
      | family      |
    And the target test has the items of the context
    And the aggregation test_agg is parameterized
    When I build the DAGs
    Then in the DAG test_g9 finish_test should depend on merge_test_agg_test
    And in the DAG test_pl finish_test should depend on merge_test_agg_test
    And in the DAG test_g9 done should depend on finish_test
    And in the DAG test_pl done should depend on finish_test
