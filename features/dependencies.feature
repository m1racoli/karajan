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

Feature: Dependencies
  Define dependencies for column aggregations

  Background:
    Given a minimal config

  Scenario: Aggregation without dependencies
    When I build the DAGs
    Then in the DAG test aggregate_test_agg should depend on wait_for_nothing

  Scenario Outline: Supported dependencies
    Given the aggregation test_agg has a <dep_type> dependency
    When I build the DAGs
    Then in the DAG test aggregate_test_agg should depend on <task_id>

    Examples:
      | dep_type | task_id                  |
      | tracking | wait_for_test_test       |
      | delta    | wait_for_0_seconds_delta |
      | task     | wait_for_task_test_test  |

  Scenario: Delta dependency as string
    Given the aggregation test_agg has a delta dependency with the following attributes
      | delta |
      | 2m    |
    When I build the DAGs
    Then in the DAG test aggregate_test_agg should depend on wait_for_120_seconds_delta

  Scenario: Target dependency
    Given the target another_target with the aggregation another_agg
    And the aggregation test_agg has a target dependency with the following attributes
      | target         |
      | another_target |
    When I build the DAGs
    Then in the DAG test aggregate_test_agg should depend on merge_another_agg_another_target

  Scenario: Target dependency with column resolution
    Given the target another_target with the aggregation another_agg
    And the target another_target with the aggregation another_unused_agg
    And the aggregation test_agg has a target dependency with the following attributes
      | target         | columns            |
      | another_target | another_agg_column |
    When I build the DAGs
    Then in the DAG test aggregate_test_agg should depend on merge_another_agg_another_target
    And in the DAG test aggregate_test_agg should not depend on merge_another_unused_agg_another_target
