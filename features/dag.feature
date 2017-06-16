Feature: Build DAGs
  From an aggregation model I want to build Airflow DAGs reflecting the aggregation

  Scenario: No DAG
    When I build the DAGs
    Then there should be no DAGs

  Scenario: Minimal DAG
    Given a minimal config
    When I build the DAGs
    Then there should be one DAG
