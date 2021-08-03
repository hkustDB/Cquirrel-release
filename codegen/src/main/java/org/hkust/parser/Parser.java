package org.hkust.parser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGSchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

/**
 * Created by tom on 3/3/2021.
 * Copyright (c) 2021 tom
 */
public class Parser {
    private static String sql;
    private static String output_file_path;

    public Parser(String sql, String output_file_path) {
        this.sql = sql;
        this.output_file_path = output_file_path;
    }

    public static void parse() throws Exception {
        String q3 = "select\n" +
                "l_orderkey, \n" +
                "sum(l_extendedprice*(1-l_discount)) as revenue,\n" +
                "o_orderdate, \n" +
                "o_shippriority\n" +
                "from \n" +
                "customer c, \n" +
                "orders o, \n" +
                "lineitem l\n" +
                "where \n" +
                "c_mktsegment = 'BUILDING'\n" +
                "and c_custkey=o_custkey\n" +
                "and l_orderkey=o_orderkey\n" +
                "and o_orderdate < date '1995-03-15'\n" +
                "and l_shipdate > date '1995-03-15'\n" +
                "and l_receiptdate > l_commitdate \n" +
                "group by \n" +
                "l_orderkey, \n" +
                "o_orderdate, \n" +
                "o_shippriority;";

        String q6 = "select\n" +
                "sum(l_extendedprice*l_discount) as revenue\n" +
                "from \n" +
                "lineitem\n" +
                "where \n" +
                "l_shipdate >= date '1994-01-01'\n" +
                "and l_shipdate < date '1995-01-01'\n" +
                "and l_discount >= 0.05 \n" +
                "and l_discount <= 0.07\n" +
                "and l_quantity < 24;";


        String q10 = "select\n" +
                "c_custkey, \n" +
                "c_name, \n" +
                "sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
                "c_acctbal, \n" +
                "n_name, \n" +
                "c_address, \n" +
                "c_phone, \n" +
                "c_comment\n" +
                "from \n" +
                "customer, \n" +
                "orders, \n" +
                "lineitem, \n" +
                "nation\n" +
                "where \n" +
                "c_custkey = o_custkey\n" +
                "and l_orderkey = o_orderkey\n" +
                "and o_orderdate >= date '1993-10-01'\n" +
                "and o_orderdate < date '1994-01-01'\n" +
                "and l_returnflag = 'R'\n" +
                "and c_nationkey = n_nationkey\n" +
                "group by \n" +
                "c_custkey, \n" +
                "c_name, \n" +
                "c_acctbal, \n" +
                "c_phone, \n" +
                "n_name, \n" +
                "c_address, \n" +
                "c_comment;";

        String q19 = "select\n" +
                "sum(l_extendedprice * (1 - l_discount) ) as revenue\n" +
                "from \n" +
                "lineitem, \n" +
                "part\n" +
                "where\n" +
                "p_partkey = l_partkey and \n" +
                "((\n" +
                "p_brand = 'Brand#12'\n" +
                "and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \n" +
                "and l_quantity >= 1 and l_quantity <= 11\n" +
                "and p_size >= 1 and p_size <= 5\n" +
                ")\n" +
                "or \n" +
                "(\n" +
                "p_brand = 'Brand#23'\n" +
                "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n" +
                "and l_quantity >= 10 and l_quantity <= 20\n" +
                "and p_size >= 1 and p_size <= 10\n" +
                ")\n" +
                "or \n" +
                "(\n" +
                "p_brand = 'Brand#34'\n" +
                "and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n" +
                "and l_quantity >= 20 and l_quantity <= 30\n" +
                "and p_size >= 1 and p_size <= 15\n" +
                "))\n" +
                "and l_shipmode in ('AIR', 'AIR REG')\n" +
                "and l_shipinstruct = 'DELIVER IN PERSON';";


        DbType dbType = JdbcConstants.POSTGRESQL;
        //格式化输出
        String result = SQLUtils.format(sql, dbType);
        System.out.println(result); // 缺省大写格式
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        //解析出的独立语句的个数
        System.out.println("size is:" + stmtList.size());
        for (SQLStatement stmt : stmtList) {
            PGSchemaStatVisitor visitor = new PGSchemaStatVisitor();
            ExportTableAliasVisitor ASTvisitor = new ExportTableAliasVisitor(){};
            stmt.accept(visitor);
            stmt.accept(ASTvisitor);
            SQLToJSONWriter writer = new SQLToJSONWriter(output_file_path);
            System.out.println("groupByAttribute: " + ASTvisitor.groupByAttributes);
            System.out.println("aggregation: " + ASTvisitor.aggregation);
            System.out.println("table:" + ASTvisitor.table);
            String subqueryString = writer.checkIfRecursive(ASTvisitor);
            System.out.println("subqueryString: "+subqueryString);
            if (subqueryString != null) {
                ASTvisitor = new ExportTableAliasVisitor(){};
                List<SQLStatement> subList = SQLUtils.parseStatements(subqueryString, dbType);
                SQLStatement subquery = subList.iterator().next();
                subquery.accept(ASTvisitor);
            }
            SQLSelectQueryBlock j = (SQLSelectQueryBlock) ASTvisitor.selectStatement.iterator().next().getSelect().getQuery();
            System.out.println(j.getWhere());
            //System.out.println(ASTvisitor.selectItem);
            for (SQLSelectItem i : ASTvisitor.selectItem) {
                if (i.getExpr().getClass() == SQLAggregateExpr.class || i.getExpr().getClass() == SQLBinaryOpExpr.class) {
                    System.out.println(i + " true "+ i.getExpr());
                } else {
                    System.out.println(i + " false ");
                }
            }
            writer.addJoinStructure(ASTvisitor);
            writer.addRelationProcessFunction(ASTvisitor);
            writer.addAggregationFunction(ASTvisitor);
            writer.printJson();
            System.out.println(ASTvisitor.getAliasMap());
            //获取表名称
            System.out.println("Tables : " + visitor.getOriginalTables());
            //获取操作方法名称,依赖于表名称
            System.out.println("Manipulation : " + visitor.getTableStat("lineitem"));
            //获取字段名称
            System.out.println("fields : " + visitor.getColumns());
        }
    }

}
