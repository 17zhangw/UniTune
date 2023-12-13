from pathlib import Path
import os
import sqlparse
import time
import pdb
import sys

import jpype as jp
from jpype.types import *
import jpype.imports



''' Configure JAVA environment for JPype '''
base_dir = os.path.abspath(os.curdir)
local_lib_dir = os.path.join(base_dir, 'libs')

# For the first use: uncomment if `classpath.txt` need update
# Otherwise: commoent "_ = os.popen('mvn dependency:build-classpath -Dmdep.outputFile=classpath.txt').read()"
_ = os.popen('mvn dependency:build-classpath -Dmdep.outputFile=classpath.txt').read()

classpath = open(os.path.join(base_dir, 'classpath.txt'), 'r').readline().split(':')
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
from org.apache.calcite.sql.dialect import CalciteSqlDialect, MysqlSqlDialect, PostgresqlSqlDialect
from org.apache.calcite.tools import FrameworkConfig, Frameworks, Planner, RelBuilderFactory
from org.apache.calcite.util import SourceStringReader
from org.apache.calcite.sql.parser.babel import SqlBabelParserImpl
from org.apache.calcite.sql.validate import SqlConformanceEnum
from org.apache.calcite.sql.fun import SqlLibrary, SqlLibraryOperatorTableFactory
from org.apache.calcite.sql2rel import SqlToRelConverter


class Rewriter():

    def __init__(self):

        conn = DriverManager.getConnection('jdbc:calcite:fun=standard,postgresql')
        calcite_conn = conn.unwrap(CalciteConnection)
        root_schema = calcite_conn.getRootSchema()
        data_source = JdbcSchema.dataSource("jdbc:postgresql://localhost:5432/benchbase", 'org.postgresql.Driver', "admin", "")
        schema = root_schema.add("benchbase", JdbcSchema.create(root_schema, "benchbase", data_source, None, None))
        parserConfig = SqlParser.configBuilder().setConformance(SqlConformanceEnum.BABEL).setParserFactory(SqlBabelParserImpl.FACTORY).setCaseSensitive(False).build()
        converterConfig = SqlToRelConverter.config().withExpand(True)
        config = Frameworks.newConfigBuilder().defaultSchema(schema).parserConfig(parserConfig).sqlToRelConverterConfig(converterConfig).build()
        planner = Frameworks.getPlanner(config)

        self.planner = planner
        self.dialect = PostgresqlSqlDialect.DEFAULT  # PostgresqlSqlDialect.DEFAULT

    def SQL2RA(self, sql):
        self.planner.close()
        self.planner.reset()
        sql_node = self.planner.parse(SourceStringReader(sql))
        sql_node = self.planner.validate(sql_node)

        rel_root = self.planner.rel(sql_node)
        rel_node = rel_root.project()
        return self.RA2SQL(rel_node)

    def RA2SQL(self, ra):
        converter = RelToSqlConverter(self.dialect)
        # result = converter.visitInput(ra, 0)
        result = converter.visitRoot(ra)
        return str(result.asStatement().toSqlString(self.dialect).getSql())


if __name__ == "__main__":
    qs = [
        "query032-0.sql",
        "query054-0.sql",
        "query081-0.sql",
        "query092-0.sql",
    ]

    rewriter = Rewriter()
    sqls = [s for s in Path("queries/").glob("*.sql")]
    output = Path("output")
    output.mkdir(parents=True, exist_ok=True)
    for sqlfile in sqls:
        stem = sqlfile.parts[-1]
        with open(sqlfile) as f:
            s = f.read().strip()

        #if stem not in qs:
        #    with open(f"{output}/{stem}", "w") as f:
        #        f.write(s)
        #    continue

        print(sqlfile)
        sstream = [l for l in s.split(";") if len(l.strip()) > 0]
        queries = []
        for sql in sstream:
            try:
                rsql = rewriter.SQL2RA(sql) + ";"
            except Exception as e:
                print(e)
                rsql = sql + ";"

            queries.append(rsql)

        with open(f"{output}/{stem}", "w") as f:
            f.write("\n".join(queries))

    assert False
