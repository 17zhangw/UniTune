import re
import shutil
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
from psycopg.rows import dict_row
from pathlib import Path

sys.path.append('..')
from ..utils.parser import ConfigParser
from ..utils.limit import time_limit,  TimeoutException
from .base import DB
import psycopg
from psycopg.errors import QueryCanceled, InternalError
import concurrent.futures


def _force_statement_timeout(conn, timeout):
    retry = True
    while retry:
        try:
            conn.execute(f"set statement_timeout = {timeout * 1000}")
            retry = False
        except:
            pass


def run_query(conn, query_sql, timeout):
    timed_out = False
    _force_statement_timeout(conn, timeout)
    try:
        # Run the real query.
        start_time = time.time()
        conn.execute(query_sql)
        runtime = time.time() - start_time
    except QueryCanceled:
        runtime = timeout
        timed_out = True
    except InternalError:
        runtime = timeout
        timed_out = True

    _force_statement_timeout(conn, 0)
    return runtime, timed_out


def parallel_run_query(args):
    conn_str, qid, query_sql, timeout = args
    with psycopg.connect(conn_str, autocommit=True, prepare_threshold=None) as conn:
        runtime, timed_out = run_query(conn, query_sql, timeout)
    return qid, runtime, timed_out


class PostgresDB(DB):
    def __init__(self, *args, **kwargs):
        # internal metrics collect signal
        self.sql_dict = {
            'valid': {},
            'invalid': {}
        }

        super().__init__(*args, **kwargs)
        self.per_query_knobs = {}

    def _connect_str(self):
        return "host={host} port={port} dbname={dbname} user={user} password={password}".format(
                host=self.host,
                port=int(self.port),
                dbname=self.dbname,
                user=self.user,
                password=self.passwd
        )

    def _connect_db(self):
        conn = psycopg.connect(self._connect_str(), autocommit=True, prepare_threshold=None)
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
        conn = self._connect_db()
        require_checkpoint = False
        for key, val in config.items():
            if key.startswith("Q"):
                self.per_query_knobs[key] = val
            elif "_fillfactor" in key:
                tbl = key.split("_fillfactor")[0]

                with conn.cursor(row_factory=dict_row) as cursor:
                    pgc_record = [r for r in cursor.execute(f"SELECT * FROM pg_class where relname = '{tbl}'", prepare=False)][0]

                orig_ff = None
                if pgc_record["reloptions"] is not None:
                    for record in pgc_record["reloptions"]:
                        for key, value in re.findall(r'(\w+)=(\w*)', record):
                            if key == "fillfactor":
                                orig_ff = int(value)

                if orig_ff is None or orig_ff != int(val):
                    conn.execute(f"ALTER TABLE {tbl} SET (fillfactor = {val})")
                    conn.execute(f"VACUUM FULL {tbl}")
                    self.logger.debug(f"Issued vacuum {tbl} {val}")
                    require_checkpoint = True
            elif "max_worker_processes" in key:
                self.per_query_knobs["max_worker_processes"] = val

        if require_checkpoint:
            conn.execute("CHECKPOINT")
            self.logger.debug('Issued checkpoint.')

        with open(f"{self.postgres}/pgdata/postgresql.auto.conf", "w") as f:
            for key, val in config.items():
                if "_fillfactor" in key:
                    continue
                if key.startswith("Q"):
                    continue

                f.write(f"{key} = {val}")
                f.write("\n")
            f.write("shared_preload_libraries = 'pg_hint_plan'")

        self.logger.debug('Modify db config file successfully.')
        conn.close()
        return True

    def _create_index(self, table, column, name=None, advise_prefix='advisor'):
        if name is None:
            name = '%s_%s_%s' % (advise_prefix, table, column)
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
        self._modify_cnf(default_knob)

        self.logger.info('Reset Knob: Set Default knobs.')
        self._close_db()
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
        self._modify_cnf(default_knob)
        self.logger.info('Reset Knob: Set Default knobs.')

        self._close_db()
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
                cost = output[0][0][0]["Plan"]["Total Cost"]
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

    def _run_benchbase(self, workload):
        with local.cwd(workload["benchbase"]):
            code, _, _ = local["java"][
                "-jar", "benchbase.jar",
                "-b", workload["benchmark"],
                "-c", workload["benchbase_config"],
                "-d", workload["results"],
                "--execute=true"].run(retcode=None)

            assert code == 0


    def _run_workload(self, workload, filename):
        if "benchmark" in workload:
            self._run_benchbase(workload)
            return

        workload_qdir = workload["workload_qdir"]
        workload_qlist_qfile = workload["workload_qlist_qfile"]
        self.logger.info(f"Running {workload_qdir} {workload_qlist_qfile}")
        with open(filename, "w") as f:
            f.write("query\tlat(ms)\n")

            sqls = []
            with open(workload["workload_qlist_qfile"], "r") as q:
                for qfile in q:
                    qfile = qfile.strip()

                    query = qfile.split(".")[0]
                    qfile = f"{workload_qdir}/{qfile}"
                    with open(qfile, "r") as qq:
                        ss = qq.read().strip()
                    sqls.append((query, ss))

            if not workload["parallel_query_eval"]:
                run_time = []
                conn = self._connect_db()
                current_timeout = workload["workload_timeout"]
                if workload["per_query_timeout"]:
                    current_timeout = int(current_timeout / len(sqls))

                # Run serially.
                for (qid, query_sql) in sqls:
                    max_worker = self.per_query_knobs.get("max_worker_processes", 8)
                    pqkk = [(k, v) for k, v in self.per_query_knobs.items() if k.startswith(f"Q{int(qid)}_")]
                    pqkk = [f"Set({k}, {v})" for k, v in pqkk if "scanmethod" not in k and "parallel_rel" not in k]
                    pqkk.extend([f"{v}({k.split('_')[1]})" for k, v in pqkk if "scanmethod" in k])
                    pqkk.extend([f"Parallel({k} {max_worker})" for k, v in pqkk if "parallel_rel" in k and v != "sentinel"])

                    parts = query_sql.split("\n")
                    for i, p in enumerate(parts):
                        if p.lower().startswith("select"):
                            parts = parts[0:i] + "/*+ " + " ".join(pqkk) + " */" + parts[i:]
                            break
                    query_sql = "\n".join(parts)
                    if len(pqkk) > 0:
                        self.logger.debug(f"{qid}: {query_sql}")

                    runtime, timed_out = run_query(conn, query_sql, current_timeout)
                    self.logger.debug(f"{qid}: {runtime} {timed_out}")
                    run_time.append(runtime)

                    if not workload["per_query_timeout"]:
                        # Adjust remaining time.
                        current_timeout = current_timeout - runtime

                        # We are using a workload timeout for running serially.
                        if timed_out or current_timeout <= 0:
                            # We've timed out of the entire budget so stop.
                            break

                # reset the timeout to the default configuration
                _force_statement_timeout(conn, 0)
                conn.execute("drop view if exists revenue0_PID;")
                conn.close()

                for (query, _), duration in zip(sqls[:len(run_time)], run_time):
                    f.write(f"{query}\t{duration * 1000}\n")

            else:
                assert workload["per_query_timeout"]
                timeout = int(workload["workload_timeout"] / len(sqls))
                conn_str = self._connect_str()

                max_workers = workload["parallel_max_workers"] if workload["parallel_max_workers"] > 0 else None
                results = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    tasks = [(conn_str, qid, qsql, timeout) for qid, qsql in sqls]
                    futures = {executor.submit(parallel_run_query, (conn_str, qid, qsql, timeout)) for qid, qsql in sqls}
                    for future in concurrent.futures.as_completed(futures):
                        qid, runtime, _ = future.result()
                        results[qid] = runtime
                        self.logger.debug(f"{qid}: {runtime}")

                for (qid, _) in sqls:
                    assert qid in results
                    duration = results[qid]
                    f.write(f"{qid}\t{duration * 1000}\n")
