from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.exasol_operator import ExasolOperator
from airflow.operators.sensors import SqlSensor, TimeDeltaSensor, ExternalTaskSensor

from karajan.config import Config
from karajan.model import DeltaDependency, TrackingDependency, NothingDependency, TaskDependency


class BaseEngine(object):
    def init_operator(self, task_id, dag):
        return self._dummy_operator(task_id, dag)

    def dependency_operator(self, task_id, dag, dep):
        if isinstance(dep, DeltaDependency):
            return self.delta_dependency_operator(task_id, dag, dep)
        elif isinstance(dep, TrackingDependency):
            return self.tracking_dependency_operator(task_id, dag, dep)
        elif isinstance(dep, NothingDependency):
            return self.nothing_dependency_operator(task_id, dag)
        elif isinstance(dep, TaskDependency):
            return self.task_dependency_operator(task_id, dag, dep)
        else:
            raise "Dependency operator for %s not found" % type(dep)

    @staticmethod
    def delta_dependency_operator(task_id, dag, dep):
        return TimeDeltaSensor(
            task_id=task_id,
            dag=dag,
            delta=dep.delta,
        )

    def nothing_dependency_operator(self, task_id, dag):
        return self._dummy_operator(task_id, dag)

    def tracking_dependency_operator(self, task_id, dag, dep):
        return self._dummy_operator(task_id, dag)

    @staticmethod
    def task_dependency_operator(task_id, dag, dep):
        return ExternalTaskSensor(
            task_id=task_id,
            external_task_id=dep.task_id,
            external_dag_id=dep.dag_id,
            dag=dag,
        )

    def aggregation_operator(self, task_id, dag, table, agg):
        return self._dummy_operator(task_id, dag)

    def prepare_operator(self, task_id, dag, table):
        return self._dummy_operator(task_id, dag)

    def merge_operator(self, task_id, dag, table):
        return self._dummy_operator(task_id, dag)

    def cleanup_operator(self, task_id, dag, table, agg):
        return self._dummy_operator(task_id, dag)

    def done_operator(self, task_id, dag):
        return self._dummy_operator(task_id, dag)

    @staticmethod
    def _dummy_operator(task_id, dag):
        return DummyOperator(task_id=task_id, dag=dag)


def _aggregation_select_columns(table, agg, item_val=None, item_key=None):
    if item_val is not None and item_key is not None:
        key_columns = [c if c != item_key else "'%s' as %s" % (item_val, item_key) for c in table.key_columns.keys()]
    else:
        key_columns = table.key_columns.keys()
    agg_columns = ["%s as %s" % (c.src_column_name, c.column_name) for c in agg.columns.values()]
    return key_columns + agg_columns


class ExasolEngine(BaseEngine):
    def __init__(self, exasol_conn_id=None, queue='default'):
        self.exasol_conn_id = exasol_conn_id
        self.queue = queue

    def tracking_dependency_operator(self, task_id, dag, dep):
        return SqlSensor(
            task_id=task_id,
            dag=dag,
            conn_id=self.exasol_conn_id,
            queue=self.queue,
            sql="SELECT DISTINCT created_date FROM %s.%s WHERE CREATED_DATE='{{ macros.ds_add(ds, +1) }}'" % (
                dep.schema, dep.table)
        )

    _aggregation_query_template = "CREATE TABLE {agg_table} AS\n{agg_select}"

    def _aggregation_query(self, table, agg):
        if agg.parameterize:
            def sub_query(p):
                select = Config.render(agg.query, p)
                columns = ',\n'.join(_aggregation_select_columns(table, agg, p[table.item_key], table.item_key))
                return self._aggregation_select(select, columns)
            sub_queries = [sub_query(params) for params in table.param_set()]
            query = '\nUNION ALL\n'.join(sub_queries)
        else:
            select = Config.render(agg.query, table.defaults)
            columns = ',\n'.join(_aggregation_select_columns(table, agg))
            where = '\nWHERE %s in (%s)' % (table.item_key, ', '.join(["'%s'" % i[table.item_key] for i in table.items]))
            query = self._aggregation_select(select, columns, where)
        return self._aggregation_query_template.format(
            agg_table=self._aggregation_table_name(table, agg),
            agg_select=query
        )

    _aggregation_select_template = "SELECT\n{columns}\nFROM ({select}) sub {where}"

    def _aggregation_select(self, select, columns, where=''):
        return self._aggregation_select_template.format(select=select, columns=columns, where=where)

    def aggregation_operator(self, task_id, dag, table, agg):
        return ExasolOperator(
            task_id=task_id,
            exasol_conn_id=self.exasol_conn_id,
            dag=dag,
            sql=self._aggregation_query(table, agg),
            queue=self.queue,
        )

    def prepare_operator(self, task_id, dag, table):
        if not table.is_timeseries():
            return self._dummy_operator(task_id, dag)
        sql = "DELETE FROM %s WHERE %s = '{{ ds }}'" % (self._table(table), table.timeseries_key)
        return ExasolOperator(
            task_id=task_id,
            exasol_conn_id=self.exasol_conn_id,
            dag=dag,
            sql=sql,
            queue=self.queue,
        )

    # def merge_operator(self, task_id, dag, table):
    #     key_columns = table.key_columns.keys()
    #     agg_columns = table.aggregated_columns.keys()
    #     on_cols = ' AND '.join(["tbl.%s=tmp.%s" % (c, c) for c in key_columns])
    #     in_cols = ', '.join(key_columns + agg_columns)
    #     in_vals = ', '.join(["tmp.%s" % c for c in (key_columns + agg_columns)])
    #     params = {
    #         'table': self._table(table),
    #         'tmp_table': self._tmp_table(table),
    #         'on_cols': on_cols,
    #         'in_cols': in_cols,
    #         'in_vals': in_vals,
    #     }
    #     sql = """
    #     MERGE INTO {table} tbl
    #     USING (SELECT * FROM {tmp_table}) tmp
    #     ON {on_cols}
    #     WHEN NOT MATCHED THEN
    #     INSERT ({in_cols})
    #     VALUES ({in_vals})
    #     """.format(**params)
    #     return ExasolOperator(
    #         task_id=task_id,
    #         exasol_conn_id=self.exasol_conn_id,
    #         dag=dag,
    #         sql=sql,
    #         queue=self.queue,
    #     )

    def cleanup_operator(self, task_id, dag, table, agg):
        return ExasolOperator(
            task_id=task_id,
            exasol_conn_id=self.exasol_conn_id,
            dag=dag,
            sql='DROP TABLE IF EXISTS %s' % (self._aggregation_table_name(table, agg)),
            queue=self.queue,
        )

    @staticmethod
    def _table(table):
        return '%s.%s' % (table.schema, table.name)

    @staticmethod
    def _tmp_table(table):
        return '%s_tmp.%s_tmp_{{ ts_nodash }}' % (table.schema, table.name)

    @staticmethod
    def _aggregation_table_name(table, agg):
        return '%s_tmp.%s_agg_%s_{{ ds_nodash }}' % (table.schema, table.name, agg.name)
