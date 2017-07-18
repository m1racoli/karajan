Feature: Parameter columns
  # Enter feature description here

  Background:
    Given a minimal config
    And the context has the following defaults
      | platform |
      | android  |

  Scenario: No parameterization
    Given the target test has the following parameter columns
      | platform |
      | platform |
    When I build the DAGs
    Then in the DAG test fill_parameter_columns_test should depend on purge_test
    And in the DAG test done should depend on fill_parameter_columns_test

  Scenario: Parameterized context
    Given the context has the following items
      | item | family |
      | g9   | PP     |
      | pl   | JP     |
    And the target test has the following parameter columns
      | game_family |
      | family      |
    And the target test has the items of the context
    When I build the DAGs
    Then in the DAG test.g9 fill_parameter_columns_test should depend on purge_test
    And in the DAG test.pl fill_parameter_columns_test should depend on purge_test
    And in the DAG test.g9 done should depend on fill_parameter_columns_test
    And in the DAG test.pl done should depend on fill_parameter_columns_test

  Scenario: Parameterized context and aggregation
    Given the context has the following items
      | item | family |
      | g9   | PP     |
      | pl   | JP     |
    And the target test has the following parameter columns
      | game_family |
      | family      |
    And the target test has the items of the context
    And the aggregation test is parameterized
    When I build the DAGs
    Then in the DAG test.g9 fill_parameter_columns_test should depend on purge_test
    And in the DAG test.pl fill_parameter_columns_test should depend on purge_test
    And in the DAG test.g9 done should depend on fill_parameter_columns_test
    And in the DAG test.pl done should depend on fill_parameter_columns_test
