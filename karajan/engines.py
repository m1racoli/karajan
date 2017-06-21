from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.exasol_operator import ExasolOperator
from airflow.operators.sensors import SqlSensor, TimeDeltaSensor, ExternalTaskSensor

from karajan.model import DeltaDependency, TrackingDependency, NothingDependency, TaskDependency


class BaseEngine(object):
    def init_operator(self, task_id, dag, table, columns):
        return DummyOperator(task_id=task_id, dag=dag)

    def dependency_operator(self, task_id, dag, dep):
        if isinstance(dep, DeltaDependency):
            return self.delta_dependency_operator(task_id, dag, dep)
        elif isinstance(dep, TrackingDependency):
            return self.tracking_dependency_operator(task_id, dag, dep)
        elif isinstance(dep, NothingDependency):
            return self.nothing_dependency_operator(task_id, dag, dep)
        elif isinstance(dep, TaskDependency):
            return self.task_dependency_operator(task_id, dag, dep)
        else:
            raise "Dependency operator for %s not found" % type(dep)

    def delta_dependency_operator(self, task_id, dag, dep):
        return TimeDeltaSensor(
            task_id=task_id,
            dag=dag,
            delta=dep.delta,
        )

    def nothing_dependency_operator(self, task_id, dag, dep):
        return DummyOperator(task_id=task_id, dag=dag)

    def tracking_dependency_operator(self, task_id, dag, dep):
        return DummyOperator(task_id=task_id, dag=dag)

    def task_dependency_operator(self, task_id, dag, dep):
        return ExternalTaskSensor(
            task_id=task_id,
            external_task_id=dep.task_id,
            external_dag_id=dep.dag_id,
            dag=dag,
        )

    def aggregation_operator(self, task_id, dag, table, column):
        return DummyOperator(task_id=task_id, dag=dag)

    def clear_time_unit_operator(self, task_id, dag, table):
        return DummyOperator(task_id=task_id, dag=dag)

    def merge_operator(self, task_id, dag, table):
        return DummyOperator(task_id=task_id, dag=dag)

    def cleanup_operator(self, task_id, dag, table):
        return DummyOperator(task_id=task_id, dag=dag)


class ExasolEngine(BaseEngine):
    def __init__(self, exasol_conn_id=None, queue='default'):
        self.exasol_conn_id = exasol_conn_id
        self.queue = queue

    def init_operator(self, task_id, dag, table, columns):
        key_columns = ["%s %s" % (k, v.column_type) for k, v in table.key_columns.iteritems()]
        agg_columns = ["%s %s" % (k, v.column_type) for k, v in columns.iteritems()]
        col_str = ',\n'.join(key_columns + agg_columns)
        sql = """
        CREATE OR REPLACE TABLE %s (
        %s
        )
        """ % (self._tmp_table(table), col_str)
        return ExasolOperator(
            task_id=task_id,
            exasol_conn_id=self.exasol_conn_id,
            dag=dag,
            sql=sql,
            queue=self.queue,
        )

    def tracking_dependency_operator(self, task_id, dag, dep):
        return SqlSensor(
            task_id=task_id,
            dag=dag,
            conn_id=self.exasol_conn_id,
            queue=self.queue,
            sql="SELECT DISTINCT created_date FROM %s.%s WHERE CREATED_DATE='{{ macros.ds_add(ds, +1) }}'" % (
                dep.schema, dep.table)
        )

    def aggregation_operator(self, task_id, dag, table, column):
        key_columns = table.key_columns.keys()
        on_cols = ' AND '.join(["tmp.%s=agg.%s" % (c, c) for c in key_columns])
        in_cols = ', '.join(key_columns + [column.column_name])
        in_vals = ', '.join(["agg.%s" % c for c in (key_columns + ['val'])])
        params = {
            'tmp_table': self._tmp_table(table),
            'on_cols': on_cols,
            'query': column.query,
            'in_cols': in_cols,
            'in_vals': in_vals,
            'col_name': column.column_name,
        }
        sql = """
        MERGE INTO {tmp_table} tmp 
        USING ({query}) agg
        ON {on_cols}
        WHEN MATCHED THEN
        UPDATE SET {col_name}=agg.val
        WHEN NOT MATCHED THEN
        INSERT ({in_cols})
        VALUES ({in_vals})
        """.format(**params)
        return ExasolOperator(
            task_id=task_id,
            exasol_conn_id=self.exasol_conn_id,
            dag=dag,
            sql=sql,
            queue=self.queue,
        )

    def clear_time_unit_operator(self, task_id, dag, table):
        sql = "DELETE FROM %s WHERE %s = '{{ ds }}'" % (self._table(table), table.timeseries_key)
        return ExasolOperator(
            task_id=task_id,
            exasol_conn_id=self.exasol_conn_id,
            dag=dag,
            sql=sql,
            queue=self.queue,
        )

    def merge_operator(self, task_id, dag, table):
        key_columns = table.key_columns.keys()
        agg_columns = table.aggregated_columns.keys()
        on_cols = ' AND '.join(["tbl.%s=tmp.%s" % (c, c) for c in key_columns])
        in_cols = ', '.join(key_columns + agg_columns)
        in_vals = ', '.join(["tmp.%s" % c for c in (key_columns + agg_columns)])
        params = {
            'table': self._table(table),
            'tmp_table': self._tmp_table(table),
            'on_cols': on_cols,
            'in_cols': in_cols,
            'in_vals': in_vals,
        }
        sql = """
        MERGE INTO {table} tbl
        USING (SELECT * FROM {tmp_table}) tmp
        ON {on_cols}
        WHEN NOT MATCHED THEN
        INSERT ({in_cols})
        VALUES ({in_vals})
        """.format(**params)
        return ExasolOperator(
            task_id=task_id,
            exasol_conn_id=self.exasol_conn_id,
            dag=dag,
            sql=sql,
            queue=self.queue,
        )

    def cleanup_operator(self, task_id, dag, table):
        return ExasolOperator(
            task_id=task_id,
            exasol_conn_id=self.exasol_conn_id,
            dag=dag,
            sql='DROP TABLE IF EXISTS %s' % (self._tmp_table(table)),
            queue=self.queue,
        )

    @staticmethod
    def _table(table):
        return '%s.%s' % (table.schema, table.name)

    @staticmethod
    def _tmp_table(table):
        return '%s_tmp.%s_tmp_{{ ts_nodash }}' % (table.schema, table.name)
