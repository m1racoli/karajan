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

Feature: Parameterization

  Background:
    Given a minimal config

  Scenario: No parameterization
    When I build the DAGs
    Then the DAG test should have the task aggregate_test_agg

  Scenario: Parameterized context and target has items
    Given the context has the following items
      | item |
      | g9   |
    And the target test has the following items
      | item |
      | g9   |
    When I build the DAGs
    Then the DAG test_g9 should have the task aggregate_test_agg

  Scenario: Parameterized context and target has no items
    Given the context has the following items
      | item |
      | g9   |
    When I try to build the DAGs
    Then there should have been an exception KarajanValidationException
    And there should be no DAGs

  Scenario: Parameterized context and target with wildcard
    Given the context has the following items
      | item |
      | g9   |
    And the target test has wildcard items
    When I build the DAGs
    Then the DAG test_g9 should have the task aggregate_test_agg

  Scenario: Parameterized context
    Given the context has the following items
      | item |
      | g9   |
      | g9i  |
    And the target test has the items of the context
    And the aggregation test_agg has a tracking dependency
    And the aggregation test_agg has a delta dependency
    When I build the DAGs
    Then in the DAG test_g9 aggregate_test_agg should depend on wait_for_test_test
    And in the DAG test_g9i aggregate_test_agg should depend on wait_for_test_test
    And in the DAG test_g9 aggregate_test_agg should depend on wait_for_0_seconds_delta
    And in the DAG test_g9i aggregate_test_agg should depend on wait_for_0_seconds_delta
    And in the DAG test_g9 merge_test_agg_test should depend on aggregate_test_agg
    And in the DAG test_g9i merge_test_agg_test should depend on aggregate_test_agg
    And in the DAG test_g9 finish_test should depend on merge_test_agg_test
    And in the DAG test_g9i finish_test should depend on merge_test_agg_test
    And in the DAG test_g9 done should depend on finish_test
    And in the DAG test_g9i done should depend on finish_test

  Scenario: Parameterized context and aggregation
    Given the context has the following items
      | item |
      | g9   |
      | g9i  |
    And the target test has the items of the context
    And the aggregation test_agg is parameterized
    And the aggregation test_agg has a tracking dependency
    And the aggregation test_agg has a delta dependency
    When I build the DAGs
    Then in the DAG test_g9 aggregate_test_agg should depend on wait_for_test_test
    And in the DAG test_g9i aggregate_test_agg should depend on wait_for_test_test
    And in the DAG test_g9 aggregate_test_agg should depend on wait_for_0_seconds_delta
    And in the DAG test_g9i aggregate_test_agg should depend on wait_for_0_seconds_delta
    And in the DAG test_g9 merge_test_agg_test should depend on aggregate_test_agg
    And in the DAG test_g9i merge_test_agg_test should depend on aggregate_test_agg
    And in the DAG test_g9 finish_test should depend on merge_test_agg_test
    And in the DAG test_g9i finish_test should depend on merge_test_agg_test
    And in the DAG test_g9 done should depend on finish_test
    And in the DAG test_g9i done should depend on finish_test

  Scenario: Parameterized context and dependency
    Given the context has the following items
      | item |
      | g9   |
      | g9i  |
    And the target test has the items of the context
    And the aggregation test_agg has a tracking dependency with the following attributes
      | schema     |
      | {{ item }} |
    And the aggregation test_agg has a delta dependency
    When I build the DAGs
    Then in the DAG test_g9 aggregate_test_agg should depend on wait_for_g9_test
    And in the DAG test_g9i aggregate_test_agg should depend on wait_for_g9i_test
    And in the DAG test_g9 aggregate_test_agg should depend on wait_for_0_seconds_delta
    And in the DAG test_g9i aggregate_test_agg should depend on wait_for_0_seconds_delta
    And in the DAG test_g9 merge_test_agg_test should depend on aggregate_test_agg
    And in the DAG test_g9i merge_test_agg_test should depend on aggregate_test_agg
    And in the DAG test_g9 finish_test should depend on merge_test_agg_test
    And in the DAG test_g9i finish_test should depend on merge_test_agg_test
    And in the DAG test_g9 done should depend on finish_test
    And in the DAG test_g9i done should depend on finish_test

  Scenario:
  Parameterized context, aggregation and dependency
    Given the context has the following items
      | item |
      | g9   |
      | g9i  |
    And the target test has the items of the context
    And the aggregation test_agg is parameterized
    And the aggregation test_agg has a tracking dependency with the following attributes
      | schema     |
      | {{ item }} |
    And the aggregation test_agg has a delta dependency
    When I build the DAGs
    Then in the DAG test_g9 aggregate_test_agg should depend on wait_for_g9_test
    And in the DAG test_g9i aggregate_test_agg should depend on wait_for_g9i_test
    And in the DAG test_g9 aggregate_test_agg should depend on wait_for_0_seconds_delta
    And in the DAG test_g9i aggregate_test_agg should depend on wait_for_0_seconds_delta
    And in the DAG test_g9 merge_test_agg_test should depend on aggregate_test_agg
    And in the DAG test_g9i merge_test_agg_test should depend on aggregate_test_agg
    And in the DAG test_g9 finish_test should depend on merge_test_agg_test
    And in the DAG test_g9i finish_test should depend on merge_test_agg_test
    And in the DAG test_g9 done should depend on finish_test
    And in the DAG test_g9i done should depend on finish_test
