Feature: Timeseries
  Configure aggregations as timeseries

  Background:
    Given a minimal config

  Scenario: Timeseries key
    Given the table test has a timeseries key datum
    When I build the DAGs
    Then the DAG test should have the task clear_datum
