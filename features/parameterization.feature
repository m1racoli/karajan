Feature: Parameterization
  # Enter feature description here

  Background:
    Given a minimal config

  Scenario: No parameterization
    When I build the DAGs
    Then the DAG test should have the task aggregate_test

  Scenario: Table items and not parameterized column
    Given the target test has the following items
      | key  |
      | item |
    When I build the DAGs
    Then the DAG test should have the task aggregate_test

  Scenario: Table items and parameterized column
    Given the target test has the following items
      | key  |
      | item |
    And the aggregation test is parameterized
    When I build the DAGs
    Then the DAG test should have the task aggregate_test
