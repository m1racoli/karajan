import logging
from datetime import date

from airflow.hooks.jdbc_hook import JdbcHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.operators.sensors import SqlSensor, TimeDeltaSensor, ExternalTaskSensor

from karajan.dependencies import *
from karajan.model import AggregatedColumn
from karajan.operators import *


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

    @staticmethod
    def _dummy_operator(task_id, dag):
        return DummyOperator(task_id=task_id, dag=dag)

    def aggregate(self, tmp_table_name, columns, query, where=None):
        """

        :type tmp_table_name: str
        :type columns: list
        :type query: str
        :type where: dict
        """
        raise NotImplementedError()

    def cleanup(self, tmp_table_name):
        """

        :type tmp_table_name: str
        """
        raise NotImplementedError()

    def describe(self, tmp_table_name):
        """

        :type tmp_table_name: str
        :rtype: dict
        """
        raise NotImplementedError()

    def bootstrap(self, schema_name, table_name, columns):
        """

        :type schema_name: str
        :type table_name: str
        :type columns: dict
        """
        raise NotImplementedError()

    def delete_timeseries(self, schema_name, table_name, columns, where=None):
        """

        :type schema_name: str
        :type table_name: str
        :type columns: list
        :type where: dict
        """
        raise NotImplementedError()

    def merge(self, tmp_table_name, schema_name, table_name, key_columns, value_columns, update_types=None):
        """

        :type tmp_table_name: str
        :type schema_name: str
        :type table_name: str
        :type key_columns: dict
        :type value_columns: dict
        :type update_types: dict
        """
        raise NotImplementedError()


class ExasolEngine(BaseEngine):
    def __init__(self, tmp_schema, conn_id=None, queue='default', retries=12, retry_delay=timedelta(seconds=300),
                 autocommit=True):
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

    def prepare_operator(self, dag, agg, target, item):
        if not target.is_timeseries():
            return self._dummy_operator(self._prepare_operator_id(agg, target), dag)
        set_cols = ', '.join("%s = NULL" % c for c in target.aggregated_columns(agg.name))
        start_date = '{{ ds }}' if (agg.reruns + agg.offset == 0) else '{{ macros.ds_add(ds, -%i) }}' % (
            agg.reruns + agg.offset)
        end_date = '{{ ds }}' if agg.offset == 0 else '{{ macros.ds_add(ds, -%i) }}' % agg.offset
        where_item = ' AND %s = %s' % (agg.context.item_column, self.db_str(item)) if item else ''
        sql = "UPDATE {target_table} SET {set_cols} WHERE {timeseries_col} BETWEEN '{start_date}' AND '{end_date}'{where_item}".format(
            target_table=target.table(),
            set_cols=set_cols,
            timeseries_col=target.timeseries_key,
            where_item=where_item,
            start_date=start_date,
            end_date=end_date,
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



    def cleanup_operator(self, dag, agg, item):
        return JdbcOperator(
            task_id=self._cleanup_operator_id(agg),
            jdbc_conn_id=self.conn_id,
            dag=dag,
            sql='DROP TABLE IF EXISTS %s' % (self._aggregation_table_name(dag, agg)),
            autocommit=self.autocommit,
            **self.task_attributes
        )

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

    # new operator design

    @staticmethod
    def db_str(val):
        if isinstance(val, (str, unicode, date, datetime)):
            return "'%s'" % val
        else:
            return val

    @staticmethod
    def _where(d):
        if not d:
            return ''

        def clause(col, val):
            if isinstance(val, tuple):
                return "%s BETWEEN %s AND %s" % (col, ExasolEngine.db_str(val[0]), ExasolEngine.db_str(val[1]))
            elif isinstance(val, list):
                return "%s IN (%s)" % (col, ', '.join([ExasolEngine.db_str(v) for v in val]))
            else:
                return "%s = %s" % (col,  ExasolEngine.db_str(val))

        return "WHERE %s" % (' AND '.join([clause(c, v) for c, v in d.iteritems()]))

    def _execute(self, sql):
        logging.info('Executing: ' + str(sql))
        hook = JdbcHook(jdbc_conn_id=self.conn_id)
        hook.run(sql, self.autocommit)

    def _select(self, sql):
        logging.info('Querying: ' + str(sql))
        hook = JdbcHook(jdbc_conn_id=self.conn_id)
        return hook.get_records(sql)

    def aggregate(self, tmp_table_name, columns, query, where=None):
        sql = "CREATE TABLE {schema}.{table} AS SELECT {columns} FROM ({query}) sub {where}".format(
            schema=self.tmp_schema,
            table=tmp_table_name,
            columns=', '.join(columns),
            query=query,
            where=self._where(where),
        )
        self._execute(sql)

    def cleanup(self, tmp_table_name):
        sql = 'DROP TABLE IF EXISTS {tmp_schema}.{tmp_table}'.format(
            tmp_schema=self.tmp_schema,
            tmp_table=tmp_table_name,
        )
        self._execute(sql)

    def _describe_columns(self, schema, table):
        sql = "SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS WHERE COLUMN_TABLE = '{table}' AND COLUMN_SCHEMA = '{schema}'".format(
            table=table.upper(),
            schema=schema.upper(),
        )
        return {row[0].lower(): row[1] for row in self._select(sql)}

    def describe(self, tmp_table_name):
        return self._describe_columns(self.tmp_schema, tmp_table_name)

    def bootstrap(self, schema_name, table_name, columns):
        """

        :type schema_name: str
        :type table_name: str
        :type columns: dict
        """
        result = self._describe_columns(schema_name, table_name)
        if not result:
            # table does not exist
            ddl = "CREATE TABLE {schema}.{table} ({col_defs})".format(
                table=table_name.upper(),
                schema=schema_name.upper(),
                col_defs=', '.join("%s %s DEFAULT NULL" % (c.upper(), t) for c, t in columns.iteritems())
            )
            self._execute(ddl)
        else:
            # table exists
            ddl = []
            for column in columns:
                if column not in result:
                    ddl.append("ALTER TABLE {schema}.{table} ADD COLUMN {col} {ctype} DEFAULT NULL".format(
                        schema=schema_name.upper(),
                        table=table_name.upper(),
                        col=column.upper(),
                        ctype=columns[column],
                    ))
            if ddl:
                self._execute(ddl)

    def delete_timeseries(self, schema_name, table_name, columns, where=None):
        sql = "UPDATE {schema}.{table} SET {columns} {where}".format(
            schema=schema_name,
            table=table_name,
            columns=', '.join(["%s = NULL" % c for c in columns]),
            where=self._where(where),
        )
        self._execute(sql)

    def merge(self, tmp_table_name, schema_name, table_name, key_columns, value_columns, update_types=None):
        def update_op(col, src_col, update_type):
            if update_type == 'REPLACE':
                return "tbl.{col} = IFNULL(tmp.{src_col}, tbl.{col})".format(
                    col=col, src_col=src_col)
            elif update_type == 'KEEP':
                return "tbl.{col} = IFNULL(tbl.{col}, tmp.{src_col})".format(
                    col=col, src_col=src_col)
            elif update_type == 'MIN':
                return "tbl.{col} = COALESCE(LEAST(tbl.{col}, tmp.{src_col}), tbl.{col}, tmp.{src_col})".format(
                    col=col, src_col=src_col)
            elif update_type == 'MAX':
                return "tbl.{col} = COALESCE(GREATEST(tbl.{col}, tmp.{src_col}), tbl.{col}, tmp.{src_col})".format(
                    col=col, src_col=src_col)
            return None

        if update_types:
            set_cols = ', '.join([update_op(col, src, update_types[col]) for col, src in value_columns.iteritems()])
        else:
            set_cols = ', '.join(["tbl.%s = tmp.%s" % (col, src) for col, src in value_columns.iteritems()])

        sql = """MERGE INTO {schema}.{table} tbl
USING (SELECT {src_cols} FROM {tmp_schema}.{tmp_table}) tmp
ON {on_cols}
WHEN MATCHED THEN
UPDATE SET
{set_cols}
WHEN NOT MATCHED THEN
INSERT ({in_cols})
VALUES ({in_vals})""".format(
            schema=schema_name,
            table=table_name,
            src_cols=', '.join(key_columns + value_columns.values()),
            tmp_schema=self.tmp_schema,
            tmp_table=tmp_table_name,
            on_cols=' AND '.join(["tbl.%s = tmp.%s" % (c, c) for c in key_columns]),
            set_cols=set_cols,
            in_cols=', '.join(key_columns + value_columns.keys()),
            in_vals=', '.join(["tmp.%s" % c for c in key_columns + value_columns.values()])
        )
        self._execute(sql)
