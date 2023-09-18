from pathlib import Path
import pickle
import traceback
import random
import os
import pdb
import pandas as pd
import sys
import time
import json
import logging
import threading
import numpy as np
import sqlparse
import threading
import sql_metadata
import subprocess
import gevent
import csv
import multiprocessing as mp
from abc import ABC, abstractmethod
from ConfigSpace import Configuration
from multiprocessing import Manager
from sqlparse.sql import Identifier
from shutil import copyfile
from plumbum import local
from ..utils.parser import strip_config

sys.path.append('..')
from ..utils.parser import parse_benchmark_result, is_where, flatten_comparison

sys.path.append('../..')
from envs.spaces.state_space import MetricStateSpace
import gymnasium as gym


class DB(ABC):
    def __init__(self, task_id, dbtype, host, port, user, passwd, dbname, cnf, postgres, benchbase, knob_config_file, knob_num,
                 workload_name, workload_timeout, per_query_timeout, parallel_query_eval,
                 parallel_max_workers, workload_qlist_file, workload_qdir, q_mv_file, mv_trainset_dir,
                 benchbase_config, log_path='logs', result_path='logs/results', restart_wait_time=5, **kwargs
                 ):
        # database
        self.task_id = task_id
        self.dbtype = dbtype
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.dbname = dbname
        self.cnf = cnf
        self.all_pk_fk = None
        self.all_columns = None
        self.postgres = postgres
        self.benchbase = benchbase
        self.benchbase_config = benchbase_config
     
        # logger
        self.log_path = log_path
        self.logger = self.setup_logger()
        self.result_path = result_path
        if not os.path.exists(self.result_path):
            os.mkdir(self.result_path)

        # initialize
        self.iteration = 0

        # workload
        self.workload_name = workload_name.lower()
        self.workload_timeout = float(workload_timeout)
        self.per_query_timeout = per_query_timeout == "on"
        self.parallel_query_eval = parallel_query_eval == "on"
        self.parallel_max_workers = int(parallel_max_workers)
        self.minimum_timeout = float(workload_timeout)
        self.workload_qlist_file = workload_qlist_file
        self.workload_qdir = workload_qdir
        self.q_mv_file = q_mv_file
        self.mv_trainset_dir = mv_trainset_dir
        self.workload = self.generate_workload()
        self.queries = self.get_queries()

        # knob
        self.knob_num = int(knob_num)
        self.knob_config_file = knob_config_file
        self.knob_details = self.get_knobs()
        self.restart_wait_time = restart_wait_time
        try:
            self._connect_db()
        except Exception as e:
            self._start_db()

        # index
        self.all_pk_fk = self.get_pk_fk()
        self.all_columns = self.get_all_columns()
        self.all_index_candidates = self.generate_candidates()
        self.get_all_index_sizes()

        self.reset_index()
        self.reset_knob()
        self.reset_all()

        self.state_space = MetricStateSpace(tables=self.all_columns.keys(), seed=random.randint(0, 1e6))

    @abstractmethod
    def _connect_db(self):
        pass

    @abstractmethod
    def _execute(self, sql):
        pass

    @abstractmethod
    def _fetch_results(self, sql, json=False):
        pass

    @abstractmethod
    def _close_db(self):
        pass

    @abstractmethod
    def _start_db(self, isolation=False):
        pass

    @abstractmethod
    def _modify_cnf(self, config):
        pass

    @abstractmethod
    def _create_index(self, table, col, name=None, advisor_prefix='advisor'):
        pass

    @abstractmethod
    def _drop_index(self, table, name):
        pass

    @abstractmethod
    def _analyze_table(self):
        pass

    @abstractmethod
    def _clear_processlist(self):
        pass

    @abstractmethod
    def reset_index(self, advisor_only=True, advisor_prefix='adviosr'):
        pass

    @abstractmethod
    def reset_knob(self):
        pass

    @abstractmethod
    def reset_all(self, advisor_only=True, advisor_prefix='advisor'):
        pass

    @abstractmethod
    def get_pk_fk(self):
        pass

    @abstractmethod
    def get_all_columns(self):
        pass

    @abstractmethod
    def get_all_indexes(self, advisor_only=True, advisor_prefix='adviosr'):
        pass

    @abstractmethod
    def get_data_size(self):
        pass

    @abstractmethod
    def get_index_size(self):
        pass

    @abstractmethod
    def estimate_query_cost(self, query):
        pass

    def apply_knob_config(self, knob_config):
        if len(knob_config.items()) == 0:
            self.logger.debug('No knob changes.')
            return

        try:
            knob_config = strip_config(knob_config)
        except:
            pass

        _knob_config = {}

        # check scale
        did_print = False
        for k, v in knob_config.items():
            if "." in k and k.split(".")[0] == "index":
                continue

            kv = k.split(".")[1] if "." in k else k
            if kv not in self.knob_details:
                self.logger.debug("SKIPPING {} in apply_knob_config".format(kv))
                if not did_print:
                    self.logger.debug(repr(traceback.extract_stack()))
                    did_print = True
                continue

            if self.knob_details[k.split('.')[1] if '.' in k else k]['type'] == 'integer' and self.knob_details[k.split('.')[1] if '.' in k else k]['max'] > sys.maxsize:
                _knob_config[k] = knob_config[k] * 1000
            else:
                _knob_config[k] = knob_config[k]

        flag = self._modify_cnf(_knob_config)
        if not flag:
            #copyfile(self.cnf.replace('experiment', 'default'), self.cnf)
            raise Exception('Apply knobs failed')
        self.logger.debug("Iteration {}: Knob Configuration Applied to MYCNF!".format(self.iteration))
        self.logger.debug('Knob Config: {}'.format(_knob_config))



    def apply_query_config(self, query_config):
        assert list(query_config.keys())[0] == "query.file_id"
        v = query_config[list(query_config.keys())[0]]
        if str(v) != "0":
            self.workload_qdir = "logs/workload_qdirs/" + str(v) + "/"
            self.workload_qlist_file = "logs/workload_qdirs/" + str(v) + ".txt"
            self.workload = self.generate_workload(self.workload_qdir, self.workload_qlist_file)


    def apply_config(self,type, config):
        if type == 'knob':
            self.apply_knob_config(config)
        elif type == 'index':
            self.apply_index_config(config)
        elif type == 'query':
            self.apply_query_config(config)
        elif type == 'view':
            self.apply_view_config(config)

    def apply_index_config(self, index_config):
        if len(index_config.items()) == 0:
            self.logger.debug('No index changes.')
            return
        try:
            index_config = strip_config(index_config)
        except:

            pass
        current_indexes_dict = self.get_all_indexes(advisor_only=True)
        current_indexes = current_indexes_dict.keys()
        self.logger.debug('Index Config: {}'.format(index_config))
        did_print = False
        for tab_col, v in index_config.items():
            if "." not in tab_col:
                self.logger.debug("SKIPPING due to encountered: {}".format(tab_col))
                if not did_print:
                    self.logger.debug(repr(traceback.extract_stack()))
                    did_print = True
                continue

            if v == 'on' and tab_col not in current_indexes:
                table, column = tab_col.split('.')
                self._create_index(table, column)

            if v == 'off' and tab_col in current_indexes:
                table, column = tab_col.split('.')
                name = current_indexes_dict[tab_col]
                self._drop_index(table, name)

        self._analyze_table()
        self.logger.debug("Iteration {}: Index Configuration Applied!".format(self.iteration))


    def apply_view_config(self,  view_config):
        try:
            view_config = strip_config(view_config)
        except:
            pass

        v = view_config['file_id']
        view_config = view_config['edge']


        qvdir, mvlist = dict(), set()
        with open(os.path.join(self.mv_trainset_dir , "query_mv_q_mv_index.csv")) as fpr:
            reader = csv.reader(fpr)
            i = 0
            for content in reader:
                if view_config[i]:
                    qvdir[content[0]] = content[2]
                    mvlist.add(content[1])
                i = i + 1


        for mv in mvlist:
            self.build_mv(mv)

        self.logger.info("QV list is {}".format(qvdir))
        with open(self.workload_qlist_file) as f:
            sql_types = f.readlines()
            for i in range(len(sql_types)):
                sql_type = sql_types[i].split('.')[0]
                if not sql_type  in qvdir.keys():
                    if '-' in sql_type: #not in the edge
                        sql_types[i] = sql_type.split('-')[0] + '.sql\n'
                else:
                    sql_types[i] = qvdir[sql_type] + '.sql\n'
                    copyfile(os.path.join(self.q_mv_file, sql_types[i].strip()),  os.path.join(self.workload_qdir, sql_types[i].strip()))

        self.workload_qlist_file = self.workload_qlist_file.replace(self.workload_qlist_file.split('_')[-1], str(v)) + '.txt'
        with open(self.workload_qlist_file, 'w') as f:
            f.writelines(sql_types)

        self.workload = self.generate_workload(self.workload_qdir, self.workload_qlist_file)




    def generate_workload(self, workload_qdir=None, workload_qlist_file=None):
        if workload_qdir is None:
            workload_qdir = self.workload_qdir
        if workload_qlist_file is None:
            workload_qlist_file = self.workload_qlist_file
            
        if self.workload_name in ['tpch', 'job', 'tpcds']:
            wl = {
                'type': 'read',
                'workload_qdir': workload_qdir,
                'workload_qlist_qfile': workload_qlist_file,
                'workload_timeout': self.workload_timeout,
                'per_query_timeout': self.per_query_timeout,
                'parallel_query_eval': self.parallel_query_eval,
                'parallel_max_workers': self.parallel_max_workers,
            }
        elif self.workload_name in ['tpcc']:
            wl = {
                'benchmark': self.workload_name,
                'postgres': self.postgres,
                'benchbase': self.benchbase,
                'benchbase_config': self.benchbase_config,
                'results': "/tmp/results",
            }
        else:
            raise ValueError('Invalid workload name')
        return wl

    def get_queries(self):
        queries = []
        if len(self.workload_qlist_file) == 0:
            return queries

        with open(self.workload_qlist_file, 'r') as f:
            query_list = f.read().strip().split('\n').copy()

        for q in query_list:
            qf = os.path.join(self.workload_qdir, q)
            with open(qf, 'r') as f:
                query = f.read().strip()
                queries.append(query)
        return queries

    def generate_candidates(self):
        all_used_columns = set()
        for i, sql in enumerate(self.queries):

            parser = sql_metadata.Parser(sql)
            try:
                all_used_columns.update(parser.columns)
                # columns_dict = parser.columns_dict
                # mapper = parser._columns_with_tables_aliases
                #
                # indexable_columns = {
                #     'projection': columns_dict.get('select', list()),
                #     'order_by': columns_dict.get('order_by', list()),
                #     'group_by': columns_dict.get('group_by', list()),
                #     'filter': list(),
                #     'join': list()
                # }
                #
                # # classify Filter and Join from WHERE clause
                # stmt = sqlparse.parse(sql)[0]
                # where_clause = stmt.token_matching(funcs=is_where, idx=0)
                # comparisons = flatten_comparison(where_clause)
                # for comp in comparisons:
                #     left = comp.left
                #     right = comp.right
                #     if isinstance(left, Identifier) and isinstance(right, Identifier):  # Join
                #         indexable_columns['join'].append(mapper[left.value])
                #         indexable_columns['join'].append(mapper[right.value])
                #     else:
                #         indexable_columns['filter'].append(mapper[left.value])
                #
                # used_columns = set()
                # for key in indexable_columns.keys():
                #     indexable_columns[key].sort()
                #     used_columns.update(set(indexable_columns[key]))
                #
                # all_used_columns.update(used_columns)

            except Exception as e:
                print(f'{i+1}.sql ', e)
                # print(sqlparse.format(sql, reindent=True))
                continue

        all_used_columns = list(all_used_columns)
        all_used_columns = [a.lower() for a in all_used_columns]
        result = list()

        for column in all_used_columns:
            if column == '*':
                continue
            if '.' not in column:  # no table
                for table, columns in self.all_columns.items():
                    if column in columns:
                        index = '%s.%s' % (table, column)
                        if index not in result:
                            result.append(index)
                        break
            else:
                index = column
                if index not in result:
                    result.append(index)

        result.sort()
        self.logger.info('Initialize {} Indexes'.format(len(result)))
        print(result)
        return result

    def generate_benchmark_cmd(self):
        timestamp = int(time.time())
        filename = self.result_path + '/{}.log'.format(timestamp)
        return self.workload, filename, timestamp

    def setup_logger(self):
        if not os.path.exists(self.log_path):
            os.mkdir(self.log_path)

        logger = logging.getLogger(self.task_id)
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        # formatter = logging.Formatter('[%(asctime)s:%(filename)s#L%(lineno)d:%(levelname)s]: %(message)s')
        formatter = logging.Formatter('[{}][%(asctime)s]: %(levelname)s, %(message)s'.format(logger.name))

        p_stream = logging.StreamHandler()
        p_stream.setFormatter(formatter)
        p_stream.setLevel(logging.INFO)

        f_stream = logging.FileHandler(
            os.path.join(self.log_path, '{}.log'.format(self.task_id)), mode='a', encoding='utf8')
        f_stream.setFormatter(formatter)
        f_stream.setLevel(logging.DEBUG)

        logger.addHandler(p_stream)
        logger.addHandler(f_stream)

        return logger

    def estimate(self, index_config=None):
        self.iteration += 1

        if index_config is not None:
            self.apply_index_config(index_config)

        all_cost = 0
        for query in self.queries:
            all_cost += self.estimate_query_cost(query)

        space_cost = self.get_index_size()

        return all_cost, space_cost


    def evaluate(self, config, collect_im=True):
        #return(np.random.random(), np.random.random()), 0, np.random.random(65)
        self.iteration += 1

        if isinstance(config, Configuration):
            config = config.get_dictionary()

        index_config = {}
        knob_config = {}
        view_config = {}
        workload_qlist_file = self.workload_qlist_file
        for k, v in config.items():
            if k.startswith('index.'):
                index_config[k[6:]] = v
            elif k.startswith('knob.'):
                knob_config[k[5:]] = v
            elif k.startswith('view.'):
                view_config[k]= v
            elif k.startswith('query.') and not str(v) == '' and str(v) != "0":
                self.logger.debug("Iteration {}: Query Configuration Applied!".format(self.iteration))
                q_dir = "logs/workload_qdirs/" + str(v) + "/"
                workload_qlist_file = "logs/workload_qdirs/" + str(v) + ".txt"
                self.workload = self.generate_workload(q_dir, workload_qlist_file)
        self.apply_knob_config(knob_config)

        self._close_db()
        start_success = self._start_db()
        if not start_success:
            raise Exception

        self.logger.debug('restarting mysql, sleeping for {} seconds'.format(self.restart_wait_time))
        if len(view_config.keys()):
            self.apply_view_config(view_config)

        self.apply_index_config(index_config)

        # Save some state to help us reconstruct it later.
        workload, filename, timestamp = self.generate_benchmark_cmd()
        existing_indexes = self.get_all_indexes()
        copyfile(f"{self.postgres}/pgdata/postgresql.auto.conf", f"{self.result_path}/{timestamp}.auto.conf")
        with open(f"{self.result_path}/{timestamp}.indexes.txt", "w") as f:
            for k, v in existing_indexes.items():
                f.write(f"{k} = {v}\n")

        # # collect internal metrics
        conn = self._connect_db()
        # Log pg_class out (with the reloptions)
        pd.DataFrame([r for r in conn.execute("SELECT * FROM pg_class")]).to_csv(f"{self.result_path}/{timestamp}.pg_class.csv")
        initial_metrics = self.state_space.construct_online(connection=conn)

        self.logger.debug("Iteration {}: Benchmark start, saving results to {}!".format(self.iteration, filename))

        if "benchmark" in workload:
            workload["results"] = f"{os.getcwd()}/{self.result_path}/{timestamp}/"
            Path(workload["results"]).mkdir(parents=True, exist_ok=True)

            self._close_db()
            conn.close()

            local["tar"]["cf", f"{self.postgres}/pgdata.tgz", "-C", self.postgres, "pgdata"].run()
            self._start_db()
            conn = self._connect_db()

        self._run_workload(workload, filename)

        # stop collecting internal metrics
        final_metrics = self.state_space.construct_online(connection=conn)
        delta = self.state_space.construct_metric_delta(initial_metrics, final_metrics)
        im_result = gym.spaces.utils.flatten(self.state_space, delta)
        conn.close()

        if "benchmark" in workload:
            # Restore the database.
            self._close_db()
            local["rm"]["-rf", f"{self.postgres}/pgdata"].run()
            local["mkdir"]["-m", "0700", "-p", f"{self.postgres}/pgdata"].run()
            local["tar"]["xf", f"{self.postgres}/pgdata.tgz", "-C", f"{self.postgres}/pgdata", "--strip-components", "1"].run()
            self._start_db()

        # get costs
        time.sleep(1)
        space_cost = self.get_index_size()

        if self.workload_name in ['tpch', 'job', 'tpcds']:
            dirname, _ = os.path.split(os.path.abspath(__file__))
            time_cost, lat_mean, time_cost_dir = parse_benchmark_result(filename, workload_qlist_file, self.workload_timeout, self.per_query_timeout)
            self.time_cost_dir = time_cost_dir
        elif self.workload_name in ['tpcc']:
            files = [f for f in Path(workload["results"]).rglob("*.summary.json")]
            assert len(files) == 1
            with open(files[0], "r") as f:
                data = json.load(f)
                # Negate the tps metric so we're properly "minimizing it".
                time_cost = -data["Throughput (requests/second)"]
                lat_mean = data["Latency Distribution"]["Average Latency (microseconds)"]
                self.time_cost_dir = {}
        else:
            raise ValueError

        self.logger.info("Iteration {}: configuration {}\t time_cost {}\t space_cost {}\t lat_mean {}\t timestamp {}".format(
            self.iteration, config, time_cost, space_cost, lat_mean, timestamp))

        if time_cost < self.minimum_timeout:
            self.minimum_timeout = time_cost
        return (lat_mean, time_cost), space_cost, im_result

    def im_alive_init(self):
        global im_alive
        im_alive = mp.Value('b', True)

    def set_im_alive(self, value):
        im_alive.value = value

    def get_internal_metrics(self, internal_metrics, run_time, warmup_time):
        _counter = 0
        _period = 5
        count = (run_time + warmup_time) / _period - 1
        warmup = warmup_time / _period

        def collect_metric(counter):
            counter += 1
            timer = threading.Timer(float(_period), collect_metric, (counter,))
            timer.start()
            if counter >= count or not im_alive.value:
                timer.cancel()
            if counter > warmup:
                sql = 'SELECT NAME, COUNT from information_schema.INNODB_METRICS where status="enabled" ORDER BY NAME'
                res = self._fetch_results(sql)
                im_dict = {}
                for (k, v) in res:
                    im_dict[k] = v
                internal_metrics.append(im_dict)

        collect_metric(_counter)
        return internal_metrics

    def get_knobs(self):
        blacklist = []
        with open(self.knob_config_file, 'r') as f:
            knob_tmp = json.load(f)
            knobs = list(knob_tmp.keys())

        i = 0
        count = 0
        knob_details = dict()
        while count < self.knob_num:
            key = knobs[i]
            if not key in blacklist:
                knob_details[key] = knob_tmp[key]
                count = count + 1

            i = i + 1

        self.logger.info('Initialize {} Knobs'.format(len(knob_details.keys())))
        return knob_details

    def get_all_index_sizes(self, path='/tmp/indexsize.json'):
        if os.path.exists(path):
            return

        indexsize = {}
        default = {index: 'off' for index in self.all_index_candidates}
        for index in self.all_index_candidates:
            config = default.copy()
            config[index] = 'on'
            self.apply_index_config(config)
            indexsize[index] = self.get_index_size()
            print("index {}:{}".format(index, indexsize[index] ))

        with open(path, 'w') as f:
            json.dump(indexsize, f, indent=4)
