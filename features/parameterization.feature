Feature: Parameterization
  # Enter feature description here

  Background:
    Given a minimal config

  Scenario: No parameterization
    When I build the DAGs
    Then the DAG test should have the task aggregate_test

  Scenario: Parameterized context and target has items
    Given the context has the following items
      | item |
      | g9   |
    And the target test has the following items
      | item |
      | g9   |
    When I build the DAGs
    Then the DAG test.g9 should have the task aggregate_test

  Scenario: Parameterized context and target has no items
    Given the context has the following items
      | item |
      | g9   |
    When I try to build the DAGs
    Then there should have been an exception ValidationException
    And there should be no DAGs

  Scenario: Parameterized context and target with wildcard
    Given the context has the following items
      | item |
      | g9   |
    And the target test has wildcard items
    When I build the DAGs
    Then the DAG test.g9 should have the task aggregate_test

  Scenario: Parameterized context
    Given the context has the following items
      | item |
      | g9   |
      | g9i  |
    And the target test has the items of the context
    And the aggregation test has a tracking dependency
    When I build the DAGs
    Then in the DAG test.g9 aggregate_test should depend on wait_for_test_test
    And in the DAG test.g9 prepare_test_test should depend on aggregate_test
    And in the DAG test.g9 merge_test_test should depend on prepare_test_test
    And in the DAG test.g9i aggregate_test should depend on wait_for_test_test
    And in the DAG test.g9i prepare_test_test should depend on aggregate_test
    And in the DAG test.g9i merge_test_test should depend on prepare_test_test
    And in the DAG test.g9i purge_test should depend on merge_test_test
    And in the DAG test.g9 purge_test should depend on merge_test_test
    And in the DAG test.g9i done should depend on purge_test
    And in the DAG test.g9 done should depend on purge_test

  Scenario: Parameterized context and aggregation
    Given the context has the following items
      | item |
      | g9   |
      | g9i  |
    And the target test has the items of the context
    And the aggregation test is parameterized
    And the aggregation test has a tracking dependency
    When I build the DAGs
    Then in the DAG test.g9i aggregate_test should depend on wait_for_test_test
    And in the DAG test.g9 aggregate_test should depend on wait_for_test_test
    And in the DAG test.g9i prepare_test_test should depend on aggregate_test
    And in the DAG test.g9 prepare_test_test should depend on aggregate_test
    And in the DAG test.g9i merge_test_test should depend on prepare_test_test
    And in the DAG test.g9 merge_test_test should depend on prepare_test_test
    And in the DAG test.g9i purge_test should depend on merge_test_test
    And in the DAG test.g9 purge_test should depend on merge_test_test
    And in the DAG test.g9i done should depend on purge_test
    And in the DAG test.g9 done should depend on purge_test

  Scenario: Parameterized context and dependency
    Given the context has the following items
      | item |
      | g9   |
      | g9i  |
    And the target test has the items of the context
    And the aggregation test has a tracking dependency with the following attributes
      | schema     |
      | {{ item }} |
    When I build the DAGs
    Then in the DAG test.g9 aggregate_test should depend on wait_for_g9_test
    And in the DAG test.g9i aggregate_test should depend on wait_for_g9i_test
    And in the DAG test.g9i prepare_test_test should depend on aggregate_test
    And in the DAG test.g9 prepare_test_test should depend on aggregate_test
    And in the DAG test.g9i merge_test_test should depend on prepare_test_test
    And in the DAG test.g9 merge_test_test should depend on prepare_test_test
    And in the DAG test.g9i purge_test should depend on merge_test_test
    And in the DAG test.g9 purge_test should depend on merge_test_test
    And in the DAG test.g9i done should depend on purge_test
    And in the DAG test.g9 done should depend on purge_test

  Scenario: Parameterized context, aggregation and dependency
    Given the context has the following items
      | item |
      | g9   |
      | g9i  |
    And the target test has the items of the context
    And the aggregation test is parameterized
    And the aggregation test has a tracking dependency with the following attributes
      | schema     |
      | {{ item }} |
    When I build the DAGs
    Then in the DAG test.g9 aggregate_test should depend on wait_for_g9_test
    And in the DAG test.g9i aggregate_test should depend on wait_for_g9i_test
    And in the DAG test.g9i prepare_test_test should depend on aggregate_test
    And in the DAG test.g9 prepare_test_test should depend on aggregate_test
    And in the DAG test.g9i merge_test_test should depend on prepare_test_test
    And in the DAG test.g9 merge_test_test should depend on prepare_test_test
    And in the DAG test.g9i purge_test should depend on merge_test_test
    And in the DAG test.g9 purge_test should depend on merge_test_test
    And in the DAG test.g9i done should depend on purge_test
    And in the DAG test.g9 done should depend on purge_test
