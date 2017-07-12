from datetime import timedelta, date, datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.operators.sensors import SqlSensor, TimeDeltaSensor, ExternalTaskSensor

from karajan.config import Config
from karajan.dependencies import DeltaDependency, TrackingDependency, NothingDependency, TaskDependency
from karajan.model import AggregatedColumn


class BaseEngine(object):
    def init_operator(self, task_id, dag, table):
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
            raise StandardError("Dependency operator for %s not found" % type(dep))

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

    def aggregation_operator(self, task_id, dag, target, agg, params, item):
        return self._dummy_operator(task_id, dag)

    def merge_operator(self, task_id, dag, table, agg, item):
        return self._dummy_operator(task_id, dag)

    def cleanup_operator(self, task_id, dag, table, agg, item):
        return self._dummy_operator(task_id, dag)

    def done_operator(self, task_id, dag):
        return self._dummy_operator(task_id, dag)

    def param_column_op(self, task_id, dag, target, params, item):
        return self._dummy_operator(task_id, dag)

    @staticmethod
    def _dummy_operator(task_id, dag):
        return DummyOperator(task_id=task_id, dag=dag)


class ExasolEngine(BaseEngine):
    def __init__(self, conn_id=None, queue='default', retries=12, retry_delay=timedelta(seconds=300), autocommit=True):
        self.conn_id = conn_id
        self.autocommit = autocommit
        self.task_attributes = {
            'retries': retries,
            'retry_delay': retry_delay,
            'queue': queue,
        }

    def tracking_dependency_operator(self, task_id, dag, dep):
        return SqlSensor(
            task_id=task_id,
            dag=dag,
            conn_id=self.conn_id,
            sql="SELECT created_date FROM %s.%s WHERE CREATED_DATE>'{{ ds }}' LIMIT 1" % (
                dep.schema, dep.table),
            **self.task_attributes
        )

    def aggregation_operator(self, task_id, dag, target, agg, params, item):
        select = Config.render(agg.query, params)
        if item is None:
            # nothing parameterized
            where = ''
            columns = target.src_column_names(agg.name)
            tmp_table = self._aggregation_table_name(target, agg)
        elif isinstance(item[1], list):
            # parameterized context
            where = "WHERE %s in (%s)" % (item[0],
                                          ', '.join([self.db_str(i) for i in item[1]]))
            columns = target.src_column_names(agg.name)
            tmp_table = self._aggregation_table_name(target, agg)
        else:
            # parameterized context and aggregation
            where = ''
            columns = [n if n != item[0] else "%s as %s" % (self.db_str(item[1]), item[0]) for n in target.src_column_names(agg.name)]
            tmp_table = self._aggregation_table_name(target, agg, item[1])
        columns = ', '.join(columns)
        sql = "CREATE TABLE {tmp_table} AS\nSELECT\n{columns} FROM ({select}) sub {where}"
        return JdbcOperator(
            task_id=task_id,
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql=sql.format(tmp_table=tmp_table, columns=columns, select=select, where=where),
            autocommit=self.autocommit,
            **self.task_attributes
        )

    def init_operator(self, task_id, dag, target):
        if not target.is_timeseries():
            return self._dummy_operator(task_id, dag)
        sql = "DELETE FROM %s WHERE %s = '{{ ds }}'" % (target.table(), target.timeseries_key)
        return JdbcOperator(
            task_id=task_id,
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql=sql,
            autocommit=self.autocommit,
            **self.task_attributes
        )

    _merge_query_template = """
MERGE INTO {target_table} tbl
USING (SELECT {src_cols} FROM {tmp_table}) tmp
ON {on_cols}
WHEN MATCHED THEN
UPDATE SET {set_cols}
WHEN NOT MATCHED THEN
INSERT ({in_cols})
VALUES ({in_vals})
        """

    def merge_operator(self, task_id, dag, target, agg, item):
        key_columns = target.key_columns
        agg_src_columns = [c.src_column_name for c in agg.columns.values()]
        src_cols = ', '.join(key_columns + agg_src_columns)
        on_cols = ' AND '.join(["tbl.%s=tmp.%s" % (c, c) for c in key_columns])
        agg_columns = [c for c in agg.columns.keys()]
        in_cols = ', '.join(key_columns + agg_columns)
        in_vals = ', '.join(["tmp.%s" % c for c in (key_columns + agg_src_columns)])

        def update_op(agg_col):
            if agg_col.update_type.upper() == AggregatedColumn.replace_update_type:
                return "\ntbl.{tbl_col_name} = IFNULL(tmp.{tmp_col_name}, tbl.{tbl_col_name})".format(
                    tbl_col_name=agg_col.name, tmp_col_name=agg_col.src_column_name)
            elif agg_col.update_type.upper() == AggregatedColumn.keep_update_type:
                return "\ntbl.{tbl_col_name} = IFNULL(tbl.{tbl_col_name}, tmp.{tmp_col_name})".format(
                    tbl_col_name=agg_col.name, tmp_col_name=agg_col.src_column_name)
            elif agg_col.update_type.upper() == AggregatedColumn.min_update_type:
                return "\ntbl.{tbl_col_name} = COALESCE(LEAST(tbl.{tbl_col_name}, tmp.{tmp_col_name}), tbl.{tbl_col_name}, tmp.{tmp_col_name})".format(
                    tbl_col_name=agg_col.name, tmp_col_name=agg_col.src_column_name)
            elif agg_col.update_type.upper() == AggregatedColumn.max_update_type:
                return "\ntbl.{tbl_col_name} = COALESCE(GREATEST(tbl.{tbl_col_name}, tmp.{tmp_col_name}), tbl.{tbl_col_name}, tmp.{tmp_col_name})".format(
                    tbl_col_name=agg_col.name, tmp_col_name=agg_col.src_column_name)
            return None

        set_cols = ', '.join([update_op(c) for c in agg.columns.values() if update_op(c) is not None])
        sql = self._merge_query_template.format(
            target_table=target.table(),
            tmp_table=self._aggregation_table_name(target, agg, item),
            on_cols=on_cols,
            src_cols=src_cols,
            in_cols=in_cols,
            in_vals=in_vals,
            set_cols=set_cols
        )
        return JdbcOperator(
            task_id=task_id,
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql=sql,
            autocommit=self.autocommit,
            depends_on_past=(not target.is_timeseries() and agg.depends_on_past()),
            **self.task_attributes
        )

    def cleanup_operator(self, task_id, dag, target, agg, item):
        return JdbcOperator(
            task_id=task_id,
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql='DROP TABLE IF EXISTS %s' % (self._aggregation_table_name(target, agg, item)),
            autocommit=self.autocommit,
            **self.task_attributes
        )

    @staticmethod
    def db_str(val):
        if isinstance(val, (str, unicode, date, datetime)):
            return "'%s'" % val
        else:
            return val

    def param_column_op(self, task_id, dag, target, params, item):
        sql = []
        for column, pname in target.parameter_columns.iteritems():
            sql.append(
                "UPDATE {schema}.{table} SET {column} = {value} WHERE ({column} IS NULL OR {column} != {value})".format(
                    table=target.name,
                    schema=target.schema,
                    column=column,
                    value=self.db_str(params[pname]),
                ))
        if item:
            sql = ["%s AND %s = %s" % (s, target.context.item_column, self.db_str(item)) for s in sql]
        return JdbcOperator(
            task_id=task_id,
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql=sql,
            autocommit=self.autocommit,
            depends_on_past=True,
            **self.task_attributes
        )

    @staticmethod
    def _aggregation_table_name(target, agg, item=None):
        if item:
            return '%s_tmp.%s_agg_%s_%s_{{ ds_nodash }}' % (target.schema, target.name, agg.name, item)
        return '%s_tmp.%s_agg_%s_{{ ds_nodash }}' % (target.schema, target.name, agg.name)
