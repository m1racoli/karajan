Feature: Validation
  Validate the configuration

  Background:
    Given a minimal config

  Scenario Outline: Required target attributes
    Given the attribute <attr> for the target test is not set
    When I try to build the DAGs
    Then there should be no DAGs
    Then there should have been an exception KarajanValidationException

    Examples:
      | attr               |
      | start_date         |
      | schema             |
      | key_columns        |
      | aggregated_columns |

  Scenario Outline: Required aggregation attributes
    Given the attribute <attr> for the aggregation test_agg is not set
    When I try to build the DAGs
    Then there should be no DAGs
    Then there should have been an exception KarajanValidationException

    Examples:
      | attr     |
      | query    |
      | time_key |
