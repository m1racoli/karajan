# bit.karajan
A conductor of aggregations in Apache Airflow

## Model

### Table

| name | required | purpose |
| ---- | -------- | ------- |
| start_date | required | start date of the DAG |
| schema | required | DB Schema of the aggregated table |
| key_columns | required | columns to merge new data on |
| aggregated_columns | required | column name -> column reference | 
| items | optional | run the aggregation for multiple items | 
| defaults | optional | set default values for the `params` object | 
| item_key | if items | the parameterization key to be used for naming and more | 

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
  items:
    - { game_key: g9i }
    - { game_key: g9, userid: fbuserid }
  defaults:
    userid: deviceid
  item_key: game_key
```

### Column

| name | required | purpose |
| ---- | -------- | ------- |
| query | required | aggregation query |
| dependencies | optional | things to wait for |
| column_type | required | column type of the aggregated column |
| paramterize | optional | if true, the aggregation will run for each item of table.items |

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
  column_type: VARCHAR(2)
  parameterize: true
```

