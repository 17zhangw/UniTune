import os
import sqlparse
import time
import pdb
import sys
sys.path.append('../query_rewrite')
from .mcts import *

import jpype as jp
from jpype.types import *
import jpype.imports



''' Configure JAVA environment for JPype '''
base_dir = os.path.abspath(os.curdir)
local_lib_dir = os.path.join(base_dir, 'MultiTune/advisor/query_rewrite/libs')

# For the first use: uncomment if `classpath.txt` need update
# Otherwise: commoent "_ = os.popen('mvn dependency:build-classpath -Dmdep.outputFile=classpath.txt').read()"
_ = os.popen('mvn dependency:build-classpath -Dmdep.outputFile=classpath.txt').read()

classpath = open(os.path.join(os.path.join(base_dir, 'MultiTune/advisor/query_rewrite/'), 'classpath.txt'), 'r').readline().split(':')
classpath.extend([os.path.join(local_lib_dir, jar) for jar in os.listdir(local_lib_dir)])
# print('\n'.join(classpath))

if not jp.isJVMStarted():
    jp.startJVM(jp.getDefaultJVMPath(), classpath=classpath)

from javax.sql import DataSource
from java.sql import Connection, DriverManager
from java.util import ArrayList, List

from org.postgresql import Driver as PostgreSQLDriver

import org.apache.calcite.rel.rules as R
from org.apache.calcite.sql.parser import SqlParser
from org.apache.calcite.adapter.jdbc import JdbcSchema
from org.apache.calcite.jdbc import CalciteConnection
from org.apache.calcite.plan import RelOptUtil, RelOptRule
from org.apache.calcite.plan.hep import HepMatchOrder, HepPlanner, HepProgram, HepProgramBuilder
from org.apache.calcite.rel import RelRoot, RelNode
from org.apache.calcite.rel.rel2sql import RelToSqlConverter

from org.apache.calcite.rel.rules import FilterJoinRule, AggregateExtractProjectRule, FilterMergeRule

from org.apache.calcite.schema import SchemaPlus
from org.apache.calcite.sql import SqlNode, SqlDialect
from org.apache.calcite.sql.dialect import CalciteSqlDialect, PostgresqlSqlDialect, MysqlSqlDialect
from org.apache.calcite.tools import FrameworkConfig, Frameworks, Planner, RelBuilderFactory
from org.apache.calcite.util import SourceStringReader


class Rewriter():

    def __init__(self, args, db):

        try:
            if planner: pass
        except:
            conn = DriverManager.getConnection('jdbc:calcite:')
            calcite_conn = conn.unwrap(CalciteConnection)
            root_schema = calcite_conn.getRootSchema()
            if db.dbtype == 'mysql':
                data_source = JdbcSchema.dataSource("jdbc:mysql://127.0.0.1:{}/{}?useSSL=false&socket=/data2/ruike/mysql8/base/mysql.sock".format(db.port, db.dbname),
                                                'com.mysql.jdbc.Driver', db.user, db.passwd)
            elif db.dbtype == 'postgres':
                data_source = JdbcSchema.dataSource("jdbc:postgresql://" + db.host + ':' + str(db.port) + '/',
                                   'org.postgresql.Driver', db.user, db.password)
            schema = root_schema.add(db.dbname, JdbcSchema.create(root_schema, db.dbname, data_source, None, None))
            # config = Frameworks.newConfigBuilder().parserConfig(SqlParser.Config.setCaseSensitive(False)).build()
            parserConfig = SqlParser.configBuilder(SqlParser.Config.DEFAULT).setCaseSensitive(False).build()
            config = Frameworks.newConfigBuilder().defaultSchema(schema).parserConfig(parserConfig).build()

            # defaultSchema(schema).parserConfig(SqlParser.configBuilder().setCaseSensitive(False).build()).build()
            planner = Frameworks.getPlanner(config)

        try:
            if dialect: pass
        except:
            dialect = MysqlSqlDialect.DEFAULT  # PostgresqlSqlDialect.DEFAULT
        # print('dialect configured')

        # rule list
        self.rulelist = []
        with open(os.path.join(base_dir,'MultiTune/advisor/query_rewrite/user_selected_rules.txt'), 'r') as rule_file:
            for rule in rule_file:
                self.rulelist.append(rule.strip())

        self.planner = planner
        self.dialect = dialect

        self.db = db

    def parse_quote(self, sql):
        new_sql = ""
        # print(sql)
        sql = str(sql)

        for token in sqlparse.parse(sql)[0].flatten():
            if token.ttype is sqlparse.tokens.Name and token.parent and not isinstance(token.parent.parent,
                                                                                       sqlparse.sql.Function):
                new_sql += '\"' + token.value + '\"'
            elif token.value != ';':
                new_sql += token.value

        return new_sql

    def SQL2RA(self, sql, ruleid):
        self.planner.close()
        self.planner.reset()
        sql_node = self.planner.parse(SourceStringReader(sql))
        sql_node = self.planner.validate(sql_node)

        rel_root = self.planner.rel(sql_node)
        import pdb
        # pdb.set_trace()
        rel_node = rel_root.project()

        sql_o = self.RA2SQL(rel_node)
        ruledir = jp.JPackage('org.apache.calcite.rel.rules')
        # print("test rule:" + self.rulelist[ruleid])
        rule = eval(self.rulelist[ruleid])

        if ruleid == -1:
            program = HepProgramBuilder().addMatchOrder(HepMatchOrder.TOP_DOWN).build()

            return 1, rel_node
        else:
            # judge whether the rule can be applied to the query (rel_node)
            if rule.getOperand().matches(rel_node) == True:
                program = HepProgramBuilder().addMatchOrder(HepMatchOrder.TOP_DOWN).build()
                hep_planner = HepPlanner(program)

                # hep_planner.addRule(PruneEmptyRules.PROJECT_INSTANCE)
                hep_planner.clear()
                hep_planner.addRule(rule)  # how to select one

                hep_planner.setRoot(rel_node)
                rel_node2 = hep_planner.findBestExp()
                # print(rule)
                if not self.RA2SQL(rel_node2) == sql_o:
                    pdb.set_trace()

                return 1, rel_node2
            else:
                return 0, rel_node


    def RA2SQL(self, ra):
        converter = RelToSqlConverter(self.dialect)
        # result = converter.visitInput(ra, 0)
        result = converter.visitRoot(ra)
        return str(result.asStatement().toSqlString(self.dialect).getSql())

    def single_rewrite(self, sql, ruleid):

        # sql = sql.replace(";", "")
        # sql = sql.lower()

        # print(' formatted sql '.center(60, '-'))

        # format_sql = self.parse_quote(sql.lower())
        format_sql = self.parse_quote(sql.lower()).replace('`', '')
        # format_sql = sql
        # print(format_sql)
        is_rewritten, ra = self.SQL2RA(format_sql, ruleid)
        if is_rewritten == 1:  # the rule can be applied
            # print(' rewritten sql '.center(60, '-'))

            sql = self.RA2SQL(ra)
            from MultiTune.utils.limit import time_limit, TimeoutException
            try:
                with time_limit(5):
                    success, res = self.db.validate_sql(str(sql))
            except  Exception as e:
                if isinstance(e, TimeoutException):
                    self.db._clear_processlist()
                    print("Timed out!")
            if success == 1:
                return 1, str(sql)
            else:
                return 0, ''
        else:
            return 0, ''


'''
    def single_rewrite_check(self, sql, ruleid):

        sql = sql.replace(";", "")
        sql = sql.lower()



        sql_node = self.planner.parse(SourceStringReader(sql))
        print(sql_node)
        sql_node = self.planner.validate(sql_node)
        rel_root = self.planner.rel(sql_node)
        rel_node = rel_root.project()

        ruledir = jp.JPackage('org.apache.calcite.rel.rules')
        # judge whether the rule works
        if eval(self.rulelist[ruleid]).getOperand().matches(rel_node) == True:
            sql = self.single_rewrite(sql, ruleid)
            # judge whether this sql is executable
            if self.db.validate_sqlsql) != '':
                return True

        return False
'''


def rewrite(args, db, estimator, origin_cost, origin_type, sql):
    rewriter = Rewriter(args, db)

    rewrite_sequence = []
    # sql = args.sql

    if args.rewrite_policy == 'topdown':  # -1
        is_rewritten, sql = rewriter.single_rewrite(sql, -1)
        rewrite_sequence = []

    elif args.rewrite_policy == 'mcts':
        # print("mcts ... \n")
        # initialize the policy tree

        current_node = Node(sql, db, estimator, origin_cost, 0, sql, origin_type, rewriter, args.gamma)  # root
        best_node = current_node

        for l in range(args.num_turns):
            # print(str(l) + ".... \n")
            best_node = UCTSEARCH(args.num_sims / (l + 1), best_node, args.parallel_num,
                                  estimator)  # select optimal rewritten query with mcts
            # print("level %d" % l)
            # print("Num Children: %d" % len(best_node.children))
            # for i, c in enumerate(best_node.children):
            #    print(i, c)
            # print("Best Child: {} {} {}".format(best_node.state, best_node.reward, best_node.rewrite_sequence))

            # print("--------------------------------")

            # drawPolicyTree(current_node, file.replace('.sql',''))

            sql = str(best_node.state)
            # print("final sql: ",sql)
            rewrite_sequence = best_node.rewrite_sequence
            # print(str(rewrite_sequence))


    else:
        print("Fault Policy!")
        exit()

    return sql, rewrite_sequence