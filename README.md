# bit.karajan
A conductor of aggregations in Apache Airflow

## Model

### Context

| name | required | purpose | default |
| ---- | -------- | ------- | ------- |
| items | optional | parameterize the aggregtions for those items |
| defaults | optional | default values used in template, can also be used without items |
| item_column | if items | column name for parameterization |

```yaml
items:
  g9i:
  g9: { userid: fbuserid }
defaults:
  userid: deviceid
item_column: game_key
```

### Targets

| name | required | purpose | default |
| ---- | -------- | ------- | ------- |
| start_date | required | start date of the DAG |
| schema | required | DB Schema of the target table |
| key_columns | required | columns to merge new data on |
| aggregated_columns | required | column name -> column reference | 
| timeseries_key | optional | if set, aggregation will be done on per time unit basis |
| items | if context.items | list of items or '*' |

#### targets.yml
```yaml
daily_user_activities:
  start_date: 2017-06-01
  schema: agg
  key_columns:
    - activity_date
    - userid
  aggregated_columns:
    user_logins:
      country:
      logins:
  timeseries_key: activity_date
  items: [g9i,g9]
```

### Aggregations

| name | required | purpose | default |
| ---- | -------- | ------- | ------- |
| query | required | aggregation query |
| dependencies | optional | a list of dependencies |
| offset | optional | date offset (>= 0) to run the aggregation for |

#### aggregations.yml
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
    WHERE CREATED_DATE BETWEEN '{{ start_date }}' AND '{{ end_date }}'
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
schema: '{{ item }}'
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
dag_id: '{{ item }}'
```


#### Target

| name | purpose |
| ---- | ------- |
| target | target table to be used as source |
| columns | optional list of columns of the target table |

```yaml
type: target
target: daily_user_activities
columns: [country]
```
