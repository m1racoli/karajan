from datetime import timedelta, date, datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.operators.sensors import SqlSensor, TimeDeltaSensor, ExternalTaskSensor

from karajan.config import Config
from karajan.dependencies import DeltaDependency, TrackingDependency, NothingDependency, TaskDependency, TargetDependency
from karajan.model import AggregatedColumn


class BaseEngine(object):
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

    def aggregation_operator(self, dag, target, agg, params, item):
        return self._dummy_operator(self._aggregation_operator_id(agg), dag)

    @staticmethod
    def _aggregation_operator_id(agg):
        return 'aggregate_%s' % agg.name

    def merge_operator(self, dag, table, agg, item):
        return self._dummy_operator(self._merge_operator_id(agg, table), dag)

    @staticmethod
    def _merge_operator_id(agg, target):
        return 'merge_%s_%s' % (agg.name, target.name)

    def cleanup_operator(self, dag, agg, item):
        return self._dummy_operator(self._cleanup_operator_id(agg), dag)

    @staticmethod
    def _cleanup_operator_id(agg):
        return 'cleanup_%s' % agg.name

    def purge_operator(self, dag, target, item):
        return self._dummy_operator(self._purge_operator_id(target), dag)

    @staticmethod
    def _purge_operator_id(target):
        return 'purge_%s' % target.name

    def param_column_op(self, dag, target, params, item):
        return self._dummy_operator(self._param_column_operator_id(target), dag)

    @staticmethod
    def _param_column_operator_id(target):
        return 'fill_parameter_columns_%s' % target.name

    def prepare_operator(self, dag, agg, target, item):
        return self._dummy_operator(self._prepare_operator_id(agg, target), dag)

    @staticmethod
    def _prepare_operator_id(agg, target):
        return 'prepare_%s_%s' % (agg.name,target.name)

    @staticmethod
    def _dummy_operator(task_id, dag):
        return DummyOperator(task_id=task_id, dag=dag)


class ExasolEngine(BaseEngine):
    def __init__(self, tmp_schema, conn_id=None, queue='default', retries=12, retry_delay=timedelta(seconds=300), autocommit=True):
        self.tmp_schema = tmp_schema
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

    def aggregation_operator(self, dag, src_column_names, agg, params, item):
        if agg.offset:
            select = Config.render(agg.query, params, {'ds': 'macros.ds_add(ds, -%i)' % agg.offset})
        else:
            select = Config.render(agg.query, params)
        if not item:
            # nothing parameterized
            where = ''
            columns = src_column_names
            tmp_table = self._aggregation_table_name(dag, agg)
        elif not agg.parameterize:
            # parameterized context
            where = "WHERE %s = %s" % (agg.context.item_column, self.db_str(item))
            columns = src_column_names
            tmp_table = self._aggregation_table_name(dag, agg)
        else:
            # parameterized context and aggregation
            where = ''
            columns = [n if n != agg.context.item_column else "%s as %s" % (self.db_str(item), agg.context.item_column) for n in src_column_names]
            tmp_table = self._aggregation_table_name(dag, agg)
        columns = ', '.join(columns)
        sql = "CREATE TABLE {tmp_table} AS\nSELECT\n{columns} FROM ({select}) sub {where}"
        return JdbcOperator(
            task_id=self._aggregation_operator_id(agg),
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql=sql.format(tmp_table=tmp_table, columns=columns, select=select, where=where),
            autocommit=self.autocommit,
            **self.task_attributes
        )

    def prepare_operator(self, dag, agg, target, item):
        if not target.is_timeseries():
            return self._dummy_operator(self._prepare_operator_id(agg, target), dag)
        set_cols = ', '.join("%s = NULL" % c for c in target.aggregated_columns(agg.name))
        date = '{{ macros.ds_add(ds, -%i) }}' % agg.offset if agg.offset else '{{ ds }}'
        where_item=' AND %s = %s' % (agg.context.item_column, self.db_str(item)) if item else ''
        sql = "UPDATE {target_table} SET {set_cols} WHERE {timeseries_col} = '{date}'{where_item}".format(
            target_table=target.table(),
            set_cols=set_cols,
            timeseries_col=target.timeseries_key,
            where_item=where_item,
            date=date,
        )
        return JdbcOperator(
            task_id=self._prepare_operator_id(agg, target),
            dag=dag,
            sql=sql,
            jdbc_conn_id=self.conn_id,
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

    def merge_operator(self, dag, agg, target, item):
        key_columns = target.key_columns
        agg_src_columns = target.src_column_names(agg.name)
        src_cols = ', '.join(agg_src_columns)
        on_cols = ' AND '.join(["tbl.%s=tmp.%s" % (c, c) for c in key_columns])
        agg_columns = target.aggregated_columns(agg.name)
        agg_column_names = [c for c in agg_columns.keys()]
        in_cols = ', '.join(key_columns + agg_column_names)
        in_vals = ', '.join(["tmp.%s" % c for c in (agg_src_columns)])

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

        set_cols = ', '.join([update_op(c) for c in agg_columns.values() if update_op(c) is not None])
        sql = self._merge_query_template.format(
            target_table=target.table(),
            tmp_table=self._aggregation_table_name(dag, agg),
            on_cols=on_cols,
            src_cols=src_cols,
            in_cols=in_cols,
            in_vals=in_vals,
            set_cols=set_cols
        )
        return JdbcOperator(
            task_id=self._merge_operator_id(agg, target),
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql=sql,
            autocommit=self.autocommit,
            depends_on_past=target.depends_on_past(agg.name),
            **self.task_attributes
        )

    def cleanup_operator(self, dag, agg, item):
        return JdbcOperator(
            task_id=self._cleanup_operator_id(agg),
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql='DROP TABLE IF EXISTS %s' % (self._aggregation_table_name(dag, agg)),
            autocommit=self.autocommit,
            **self.task_attributes
        )

    # TODO the purge logic needs to be refined. we\'ll do nothing for now due to the rare use case
    # def purge_operator(self, dag, target, item):
    #     if not target.is_timeseries():
    #         return self._dummy_operator(self._purge_operator_id(target), dag)
    #     where_item = ' AND %s = %s' % (target.context.item_column, self.db_str(item)) if item else ''
    #     where_agg_col = ' AND '.join("%s = NULL" % c for c in target.aggregated_columns())
    #     sql="DELETE FROM {target_table} WHERE {timeseries_key} = '{{{{ ds }}}}'{where_item} AND {where_agg_col}".format(
    #         target_table=target.table(),
    #         timeseries_key=target.timeseries_key,
    #         where_item=where_item,
    #         where_agg_col=where_agg_col,
    #     )
    #     return JdbcOperator(
    #         task_id=self._purge_operator_id(target),
    #         dag=dag,
    #         sql=sql,
    #         jdbc_conn_id=self.conn_id,
    #         autocommit=self.autocommit,
    #         **self.task_attributes
    #     )

    @staticmethod
    def db_str(val):
        if isinstance(val, (str, unicode, date, datetime)):
            return "'%s'" % val
        else:
            return val

    def param_column_op(self, dag, target, params, item):
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
            task_id=self._param_column_operator_id(target),
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql=sql,
            autocommit=self.autocommit,
            depends_on_past=True,
            **self.task_attributes
        )

    def _aggregation_table_name(self, dag, agg):
        return '%s.%s_agg_%s_{{ ds_nodash }}' % (self.tmp_schema, dag.dag_id.replace('.', '_'), agg.name)
