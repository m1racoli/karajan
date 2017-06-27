# bit.karajan
A conductor of aggregations in Apache Airflow

## Model

### Table

| name | required | purpose | default |
| ---- | -------- | ------- | ------- |
| start_date | required | start date of the DAG |
| schema | required | DB Schema of the aggregated table |
| key_columns | required | columns to merge new data on |
| aggregated_columns | required | column name -> column reference | 
| timeseries_key | optional | if set, aggregation will be done on per time unit basis |
| items | optional | run the aggregation for multiple items | 
| defaults | optional | set default values for the `params` object | 
| item_key | optional | must be one of the key columns and not the timeseries key. used for parameterisation of aggregations | key |

#### tables.yml
```yaml
daily_user_activities:
  start_date: 2017-06-01
  schema: agg
  key_columns:
    game_key: VARCHAR(5) UTF8
    activity_date: DATE
    userid: DECIMAL(36,0)
  aggregated_columns:
    user_logins:
      country:
      logins:
  timeseries_key: activity_date
  items:
    - { game_key: g9i }
    - { game_key: g9, userid: fbuserid }
  defaults:
    userid: deviceid
  item_key: game_key
```

### Column

| name | required | purpose | default |
| ---- | -------- | ------- | ------- |
| query | required | aggregation query |
| dependencies | optional | a list of dependencies |

#### columns.yml
```yaml
user_logins:
  query: |
    SELECT
      CREATED_DATE as activity_date,
      {{ userid }} as userid,
      LAST_VALUE(COUNTRY) as country,
      COUNT(*) as logins
    FROM
      {{ game_key }}.APP_LOGINS
    WHERE CREATED_DATE = '{{ ds }}'
    GROUP BY 1,2
  dependencies:
    - type: tracking
      schema: '{{ game_key }}'
      table: APP_LOGINS
    - type: delta
      delta: 30d
```

### Dependencies

#### Tracking

| name | purpose |
| ---- | ------- |
| schema | schema of the table to wait for |
| table | name of the table to wait for |

```yaml
type: tracking
schema: '{{ game_key }}'
table: APP_LOGINS
```

#### Delta

| name | purpose |
| ---- | ------- |
| delta | time to wait since execution period |

```yaml
type: delta
delta: 2h
```

#### Task

| name | purpose |
| ---- | ------- |
| dag_id | dag id of the task to wait for |
| task_id | task_id of the task to wait for |

```yaml
type: task
task_id: fill_bookings
dag_id: '{{ game_key }}'
```
