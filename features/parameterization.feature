Feature: Parameterization
  # Enter feature description here

  Background:
    Given a minimal config

  Scenario: No parameterization
    When I build the DAGs
    Then the DAG test should have the task aggregate_test

  Scenario: Parameterized context and target
    Given the context has the following items
      | item |
      | g9   |
    And the target test has the following items
      | item |
      | g9   |
    When I build the DAGs
    Then the DAG test should have the task aggregate_test

  Scenario: Parameterized context and non-parameterized target
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
    Then the DAG test should have the task aggregate_test
