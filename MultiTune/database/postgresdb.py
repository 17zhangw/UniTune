import random
from plumbum import local
from plumbum.commands.processes import ProcessTimedOut
import os
import pdb
import sys
import threading
import time
import subprocess
import json
import eventlet
import csv
import multiprocessing as mp
import psycopg
from pathlib import Path

sys.path.append('..')
from ..utils.parser import ConfigParser
from ..utils.limit import time_limit,  TimeoutException
from .base import DB
import psycopg
from psycopg.errors import QueryCanceled, InternalError


class PostgresDB(DB):
    def __init__(self, *args, postgres, **kwargs):
        self.postgres = postgres
        

        # internal metrics collect signal
        self.sql_dict = {
            'valid': {},
            'invalid': {}
        }

        super().__init__(*args, **kwargs)

    def _connect_db(self):
        conn = psycopg.connect(
            "host={host} port={port} dbname={dbname} user={user} password={password}".format(
                host=self.host,
                port=int(self.port),
                dbname=self.dbname,
                user=self.user,
                password=self.passwd
            ),
            autocommit=True,
            prepare_threshold=None)
        return conn

    def _execute(self, sql, conn=None):
        destruct = False
        if conn is None:
            conn = self._connect_db()
            destruct = True
        cursor = conn.cursor()
        cursor.execute(sql)
        if cursor: cursor.close()
        if conn and destruct: conn.close()

    def _fetch_results(self, sql, json=False):
        conn = self._connect_db()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            if cursor: cursor.close()
            if conn: conn.close()

            if json:
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in results]
            return results
        except Exception as e:
            print(e, sql)
            assert False
            return  None

    def _fetch_results2(self, sql, json=False):
        conn = self._connect_db()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            if cursor: cursor.close()
            if conn: conn.close()

            if json:
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in results]
            return results, True
        except Exception as e:
            print(e, sql)
            assert False
            return  None, False

    def _close_db(self):
        while True:
            self.logger.debug("Shutting down postgres...")
            _, stdout, stderr = local[f"{self.postgres}/pg_ctl"][
                "stop",
                "--wait",
                "-t", "300",
                "-D", f"{self.postgres}/pgdata"].run(retcode=None)
            time.sleep(1)
            self.logger.debug("Stop message: (%s, %s)", stdout, stderr)

            # Wait until pg_isready fails.
            retcode, _, _ = local[f"{self.postgres}/pg_isready"][
                "--host", self.host,
                "--port", int(self.port),
                "--dbname", self.dbname].run(retcode=None)

            exists = (Path(self.postgres) / "pgdata" / "postmaster.pid").exists()
            if not exists and retcode != 0:
                break

        self.logger.debug("Shutdown postgres successfully.")

    def _start_db(self, isolation=False):
        pid_lock = Path(f"{self.postgres}/pgdata/postmaster.pid")
        assert not pid_lock.exists()

        attempts = 0
        while not pid_lock.exists():
            # Try starting up.
            retcode, stdout, stderr = local[f"{self.postgres}/pg_ctl"][
                "-D", f"{self.postgres}/pgdata",
                "--wait",
                "-t", "300",
                "-l", f"{self.postgres}/pg.log",
                "start"].run(retcode=None)

            if retcode == 0 or pid_lock.exists():
                break

            self.logger.warn("startup encountered: (%s, %s)", stdout, stderr)
            attempts += 1
            if attempts >= 5:
                self.logger.error("Number of attempts to start postgres has exceeded limit.")
                return False

        # Wait until postgres is ready to accept connections.
        num_cycles = 0
        while True:
            if num_cycles >= 5:
                # In this case, we've failed to start postgres.
                self.logger.error("Failed to start postgres before timeout...")
                return False

            retcode, _, _ = local[f"{self.postgres}/pg_isready"][
                "--host", self.host,
                "--port", int(self.port),
                "--dbname", self.dbname].run(retcode=None)
            if retcode == 0:
                break

            time.sleep(1)
            num_cycles += 1
            self.logger.debug("Waiting for postgres to bootup but it is not...")

        return True

    def _modify_cnf(self, config):
        with open(f"{self.postgres}/pgdata/postgresql.auto.conf", "w") as f:
            for key, val in config.items():
                f.write(f"{key} = {val}")
                f.write("\n")

        self.logger.debug('Modify db config file successfully.')
        return True

    def _create_index(self, table, column, name=None, advise_prefix='advisor'):
        if name is None:
            name = '%s_%s' % (advise_prefix, column)
        sql = "CREATE INDEX %s ON %s (%s);" % (name, table, column)
        try:
            self._execute(sql)
            self.logger.debug('[success] %s' % sql)
        except Exception as e:
            self.logger.debug('[failed] %s %s' % (sql, e))

    def _drop_index(self, table, name):
        sql = "DROP INDEX %s" % (name)
        try:
            self._execute(sql)
            self.logger.debug('[success] %s' % sql)
        except Exception as e:
            self.logger.debug('[failed] %s %s' % (sql, e))

    def _analyze_table(self):
        sql = "VACUUM ANALYZE {};"
        conn = self._connect_db()
        for table in self.all_columns.keys():
            conn.execute(sql.format(table))
        conn.close()

    def _clear_processlist(self):
        # Don't support this for now.
        assert False

    def reset_index(self, advisor_only=True, advisor_prefix='advisor'):
        all_indexes_dict = self.get_all_indexes(advisor_only, advisor_prefix)
        all_indexes = all_indexes_dict.keys()

        for tab_col in all_indexes:
            if tab_col in self.all_pk_fk:
                continue
            table = tab_col.split('.')[0]
            self._drop_index(table, all_indexes_dict[tab_col])

        self._analyze_table()
        self.logger.debug('Reset Index: Drop all indexes, advisor_only={}!'.format(advisor_only))
        self._close_db()
        self._start_db()

    def reset_knob(self):
        default_knob = {knob: self.knob_details[knob]['default'] for knob in self.knob_details.keys()}
        self._close_db()
        self._modify_cnf(default_knob)
        self.logger.info('Reset Knob: Set Default knobs.')
        self._start_db()

    def reset_all(self, advisor_only=True, advisor_prefix='advisor'):
        all_indexes_dict = self.get_all_indexes(advisor_only, advisor_prefix)
        all_indexes = all_indexes_dict.keys()

        for tab_col in all_indexes:
            if tab_col in self.all_pk_fk:
                continue
            table = tab_col.split('.')[0]
            self._drop_index(table, all_indexes_dict[tab_col])

        self._analyze_table()
        self.logger.info('Reset Index: Drop all indexes, advisor_only={}!'.format(advisor_only))

        default_knob = {knob: self.knob_details[knob]['default'] for knob in self.knob_details.keys()}
        self._close_db()
        self._modify_cnf(default_knob)
        self.logger.info('Reset Knob: Set Default knobs.')
        self._start_db()

    def get_pk_fk(self):
        sql = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='public';"
        results = self._fetch_results(sql, json=False)
        return ['{}.{}'.format(row[0], row[1]) for row in results]

    def get_all_columns(self):
        sql = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='public' ORDER BY TABLE_NAME, ORDINAL_POSITION;"
        result = self._fetch_results(sql, json=False)

        columns_dict = {}
        for row in result:
            table, column = row
            if table not in columns_dict.keys():
                columns_dict[table] = list()
            columns_dict[table].append(column)
        return columns_dict

    def get_all_indexes(self, advisor_only=True, advisor_prefix='advisor'):
        sql = """
        select t.relname as table_name,
               i.relname as index_name,
               a.attname as column_name
        from pg_class t, pg_class i, pg_index ix, pg_attribute a
        where t.oid = ix.indrelid
          and i.oid = ix.indexrelid
          and a.attrelid = t.oid
          and a.attnum = ANY(ix.indkey)
          and t.relkind = 'r'
          and t.relname = '{}'
     order by t.relname, i.relname;
        """
        indexes = {}
        for table in self.all_columns.keys():
            results = self._fetch_results(sql.format(table), json=False)
            for row in results:
                name, column = row[1], row[2]
                if not advisor_only:
                    indexes['%s.%s' % (table, column)] = name
                elif name.startswith(advisor_prefix):
                    indexes['%s.%s' % (table, column)] = name
        return indexes

    def get_data_size(self):
        sql = """
            SELECT ROUND(SUM(pg_table_size(table_name::regclass)) / (1024 * 1024), 2) AS "Total Data Size"
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'public'
        """
        data_size = self._fetch_results(sql, json=False)[0][0]
        return float(data_size)

    def get_index_size(self):
        sql = """
            SELECT ROUND(SUM(pg_indexes_size(table_name::regclass)) / (1024 * 1024), 2) AS "Total Index Size"
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'public'
        """
        index_size = self._fetch_results(sql, json=False)[0][0]
        return float(index_size)

    def estimate_query_cost(self, query):
        sql = "EXPLAIN (FORMAT JSON) {}".format(query)
        try:
            with time_limit(5):
                output = self._fetch_results(sql, json=False)
                explain = json.loads(output[0][0])
                cost = explain[0]["Plan"]["Total Cost"]
        except Exception as e:
            cost = 0
            if isinstance(e, TimeoutException):
                self.logger.info("Timed out!")
                # self._clear_processlist()
            else:
                self.logger.info('{}: Exception when calling objective function: {}'.format(type, e))

        return cost

    def validate_sql(self, sql):
        sql = sql.replace("FALSE IS NULL DESC, FALSE DESC,","")
        sql = sql.replace(", FALSE IS NULL DESC, FALSE DESC", "")
        sql = sql.replace(", FALSE IS NULL, FALSE", "")
        sql = sql.replace("FALSE IS NULL, FALSE,", "")
        if sql in self.sql_dict['valid'].keys():
            return 1, self.sql_dict['valid'][sql]
        elif sql in self.sql_dict['invalid'].keys():
            #pdb.set_trace()
            return 0, ''

        conn = self._connect_db()
        cur = conn.cursor()
        fail = 1
        i=0
        cnt = 3
        while fail == 1 and i<cnt:
            try:
                fail = 0
                cur.execute('EXPLAIN (FORMAT JSON) ' + sql)
            except Exception as e:
                fail = 1
                #print(e)
                #print(sql+'\n')
            res = []
            if fail == 0:
                res = cur.fetchall()
            i = i + 1

        if fail == 1:
            #print("SQL Execution Fatal!!")
            self.sql_dict['invalid'][sql] = ''
            return 0, ''
        elif fail == 0:
            self.sql_dict['invalid'][sql] = res
            return 1, res

    def build_mv(self, mv_id):
        self.logger.info("loading " + mv_id)
        with open(os.path.join(self.q_mv_file, mv_id + ".sql"), "r") as fp:
            sql = fp.read()

        self.logger.info("executing " + mv_id + ":\n" + "CREATE TABLE " + mv_id +
                  " " + sql)
        self._execute("DROP TABLE IF EXISTS " + mv_id + ";")
        self._execute("CREATE TABLE " + mv_id + " " + sql)

    def drop_mv(self, mv_id):
        self.logger.info("dropping " + mv_id)
        self._execute("DROP TABLE " + mv_id + ";")

    def execute_from_file(self, sql_id, time_out=600):
        self.logger.info("loading " + sql_id)
        with open(os.path.join(self.q_mv_file , sql_id + ".sql"), "r") as fp:
            sql = fp.read()

        self.logger.info("executing " + sql_id)
        tstart = time.time()
        tend = -1
        conn = self._connect_db()

        try:
            with time_limit(int(time_out)):
                self._execute(sql)
                tend = time.time()
        except TimeoutException as e:
            print("Timed out!")

        if conn is not None:
            conn.close()

        if tend == -1:
            self.logger.info("timeout!")
            return time_out
        else:
            self.logger.info("successfully executed " + sql_id + " using " +
                      str(tend - tstart) + " seconds.")
            return tend - tstart

    def _run_workload(self, workload, filename):
        def _force_statement_timeout(conn, timeout):
            retry = True
            while retry:
                try:
                    conn.execute(f"set statement_timeout = {timeout * 1000}")
                    retry = False
                except:
                    pass

        with open(filename, "w") as f:
            f.write("query\tlat(ms)\n")

            conn = self._connect_db()

            workload_qdir = workload["workload_qdir"]
            sqls = []
            with open(workload["workload_qlist_qfile"], "r") as q:
                for qfile in q:
                    qfile = qfile.strip()

                    query = qfile.split(".")[0]
                    qfile = f"{workload_qdir}/{qfile}"
                    with open(qfile, "r") as qq:
                        ss = qq.read().strip()
                    sqls.append((query, ss))

            run_time = []
            current_timeout = workload["workload_timeout"]
            if workload["per_query_timeout"]:
                current_timeout = int(current_timeout / len(sqls))

            for (query, query_sql) in sqls:
                #try:
                #    # Run the warmup query.
                #    conn.execute("set statement_timeout = %d" % (current_timeout * 1000))
                #    conn.execute(query_sql)
                #except:
                #    conn.execute("drop view if exists revenue0_PID;")

                try:
                    # Run the real query.
                    self.logger.debug(f"current timeout: {current_timeout}")
                    _force_statement_timeout(conn, current_timeout)

                    start_time = time.time()
                    conn.execute(query_sql)
                    finish_time = time.time()
                    duration = finish_time - start_time
                except QueryCanceled:
                    # Only break out if we are using a "workload timeout".
                    if not workload["per_query_timeout"]:
                        break
                    else:
                        duration = current_timeout
                except InternalError:
                    # error to run the query, set duration to a large number
                    self.logger.debug(f"Internal Error for query {query_sql}")
                    if not workload["per_query_timeout"]:
                        break
                    else:
                        duration = current_timeout

                self.logger.debug(f"duration: {duration}")
                run_time.append(duration)
                if not workload["per_query_timeout"]:
                    current_timeout = current_timeout - duration

            # reset the timeout to the default configuration
            _force_statement_timeout(conn, 0)
            conn.execute("drop view if exists revenue0_PID;")
            self.logger.debug(f"runtime {run_time}")
            conn.close()

            for (query, _), duration in zip(sqls[:len(run_time)], run_time):
                f.write(f"{query}\t{duration * 1000}\n")
