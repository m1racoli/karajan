Feature: Dependencies
  Define dependencies for column aggregations

  Background:
    Given a minimal config

  Scenario Outline: Supported dependencies
    Given the aggregation test has a <dep_type> dependency
    When I build the DAGs
    Then the DAG test should have the task <task_id>

    Examples:
      | dep_type | task_id                  |
      | tracking | wait_for_test_test       |
      | delta    | wait_for_0_seconds_delta |
