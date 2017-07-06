Feature: Dependencies
  Define dependencies for column aggregations

  Background:
    Given a minimal config

  Scenario: Aggregation without dependencies
    When I build the DAGs
    Then in the DAG test aggregate_test should depend on wait_for_nothing

  Scenario Outline: Supported dependencies
    Given the aggregation test has a <dep_type> dependency
    When I build the DAGs
    Then in the DAG test aggregate_test should depend on <task_id>

    Examples:
      | dep_type | task_id                  |
      | tracking | wait_for_test_test       |
      | delta    | wait_for_0_seconds_delta |
      | task     | wait_for_task_test_test  |

  Scenario: Delta dependency as string
    Given the aggregation test has a delta dependency with the following attributes
      | delta |
      | 2m    |
    When I build the DAGs
    Then in the DAG test aggregate_test should depend on wait_for_120_seconds_delta
