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
    Then in the DAG test merge_parameter_columns should depend on merge_test
    And in the DAG test done should depend on merge_parameter_columns

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
    Then in the DAG test merge_parameter_columns_g9 should depend on merge_test
    And in the DAG test merge_parameter_columns_pl should depend on merge_test
    And in the DAG test done should depend on merge_parameter_columns_g9
    And in the DAG test done should depend on merge_parameter_columns_pl

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
    Then in the DAG test merge_parameter_columns_g9 should depend on merge_test_g9
    And in the DAG test merge_parameter_columns_pl should depend on merge_test_pl
    And in the DAG test done should depend on merge_parameter_columns_g9
    And in the DAG test done should depend on merge_parameter_columns_pl
