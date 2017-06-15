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
| item_key | optional | key of the item in items to be used for naming and more | key |

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
    country: user_country
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
| column_type | required | column type of the aggregated column |
| dependencies | optional | a list of dependencies |
| paramterize | optional | if true, the aggregation will run for each item of table.items | false |

#### columns.yml
```yaml
user_country:
  query: |
    SELECT
      '{{ params.game_key }}' as game_key,
      CREATED_DATE as activity_date,
      {{ params.userid }} as userid,
      LAST_VALUE(COUNTRY) as value
    FROM
      {{ params.game_key }}.APP_LOGINS
    WHERE CREATED_DATE = '{{ ds }}'
    GROUP BY 1,2,3
  dependencies:
    - type: tracking
      schema: '{{ params.game_key }}'
      table: APP_LOGINS
    - type: delta
      delta: 30d
  column_type: VARCHAR(2)
  parameterize: true
```

### Dependencies

#### Tracking

| name | purpose |
| ---- | ------- |
| schema | schema of the table to wait for |
| table | name of the table to wait for |

```yaml
type: tracking
schema: '{{ params.game_key }}'
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
