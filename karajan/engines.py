import logging
from datetime import date

from airflow.hooks.jdbc_hook import JdbcHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.operators.sensors import SqlSensor, TimeDeltaSensor, ExternalTaskSensor

from karajan.dependencies import *
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

    @staticmethod
    def _dummy_operator(task_id, dag):
        return DummyOperator(task_id=task_id, dag=dag)

    # new interface

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

    def purge(self, schema_name, table_name, value_columns, where):
        """

        :type schema_name: str
        :type table_name: str
        :type value_columns: list
        :type update_types: dict
        """
        raise NotImplementedError()

    def parameters(self, schema_name, table_name, parameter_columns, where):
        """

        :type schema_name: str
        :type table_name: str
        :type parameter_columns: dict
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

    # new interface

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
            src_cols=', '.join(key_columns.values() + value_columns.values()),
            tmp_schema=self.tmp_schema,
            tmp_table=tmp_table_name,
            on_cols=' AND '.join(["tbl.%s = tmp.%s" % (t, s) for t, s in key_columns.iteritems()]),
            set_cols=set_cols,
            in_cols=', '.join(key_columns.keys() + value_columns.keys()),
            in_vals=', '.join(["tmp.%s" % c for c in key_columns.values() + value_columns.values()])
        )
        self._execute(sql)

    def purge(self, schema_name, table_name, value_columns, where):
        sql = "DELETE FROM {schema}.{table} {where} {columns}".format(
            schema=schema_name,
            table=table_name,
            where=self._where(where),
            columns=' '.join("AND %s IS NULL" % c for c in value_columns)
        )
        self._execute(sql)

    def parameters(self, schema_name, table_name, parameter_columns, where):
        sql = []
        for col, val in parameter_columns.iteritems():
            sql.append("UPDATE {schema}.{table} SET {col} = {val} {where} AND ({col} IS NULL OR {col} != {val})".format(
                schema=schema_name,
                table=table_name,
                col=col,
                val=self.db_str(val),
                where=self._where(where)
            ))
        self._execute(sql)
