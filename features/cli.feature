Feature: CLI
  Control Karajan DAGs over the command line

  Background:
    Given a minimal config

  @db
  Scenario: Run
    When I trigger a Karajan run
    Then there should be an active DAG run

  @db
  Scenario: Nothing
    Then there should be no active DAG runs

  Scenario: Run with start date

  Scenario: Run with end date

  Scenario: Run with items

  @db
  Scenario: Run for target column
    When I trigger a Karajan run for a target column
    Then there should be an active DAG run
    And the DAG run should be limited to the target column and it's source column