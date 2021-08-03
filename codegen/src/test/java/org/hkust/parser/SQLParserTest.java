package org.hkust.parser;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class SQLParserTest {
    private Parser SQLParser;
    private final String TEST_GENERATED_JSON_PATH = "./test.json";
    private final String TEST_INFORMATION_JSON_PATH = "./information.json";

    @Test
    public void SQLParserQ3Test() throws Exception {
        String q3sql = "select\n" +
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

        String expected_generated_json = "{\n" +
                "  \"AggregateProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"orderkey\",\n" +
                "        \"o_orderdate\",\n" +
                "        \"o_shippriority\"\n" +
                "      ],\n" +
                "      \"AggregateValue\": [\n" +
                "        {\n" +
                "          \"value_type\": \"Double\",\n" +
                "          \"name\": \"revenue\",\n" +
                "          \"aggregation\": \"+\",\n" +
                "          \"value\": {\n" +
                "            \"right_field\": {\n" +
                "              \"right_field\": {\n" +
                "                \"name\": \"l_discount\",\n" +
                "                \"type\": \"attribute\",\n" +
                "                \"relation\": \"lineitem\"\n" +
                "              },\n" +
                "              \"left_field\": {\n" +
                "                \"var_type\": \"int\",\n" +
                "                \"type\": \"constant\",\n" +
                "                \"value\": 1\n" +
                "              },\n" +
                "              \"type\": \"expression\",\n" +
                "              \"operator\": \"-\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_extendedprice\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"*\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"output_key\": [\n" +
                "        \"orderkey\",\n" +
                "        \"o_orderdate\",\n" +
                "        \"o_shippriority\"\n" +
                "      ],\n" +
                "      \"name\": \"QAggregate0\",\n" +
                "      \"delta_output\": true\n" +
                "    }\n" +
                "  ],\n" +
                "  \"join_structure\": [\n" +
                "    {\n" +
                "      \"primary\": \"orders\",\n" +
                "      \"foreign\": \"lineitem\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"primary\": \"customer\",\n" +
                "      \"foreign\": \"orders\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"RelationProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"orderkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Date\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"1995-03-15\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_shipdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \">\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"name\": \"l_commitdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_receiptdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \">\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qlineitem\",\n" +
                "      \"next_key\": [\n" +
                "        \"orderkey\",\n" +
                "        \"o_orderdate\",\n" +
                "        \"o_shippriority\"\n" +
                "      ],\n" +
                "      \"is_Root\": true,\n" +
                "      \"child_nodes\": 1,\n" +
                "      \"relation\": \"lineitem\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Date\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"1995-03-15\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"o_orderdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"orders\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"<\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qorders\",\n" +
                "      \"next_key\": [\n" +
                "        \"orderkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 1,\n" +
                "      \"relation\": \"orders\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"varchar\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"BUILDING\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"c_mktsegment\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"customer\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"==\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qcustomer\",\n" +
                "      \"next_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 0,\n" +
                "      \"relation\": \"customer\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String expected_information_json = "{\n" +
                "  \"binary\": [\n" +
                "    \"c_custkey = o_custkey\",\n" +
                "    \"l_orderkey = o_orderkey\",\n" +
                "    \"l_receiptdate > l_commitdate\"\n" +
                "  ],\n" +
                "  \"aggregation\": [\n" +
                "    \"revenue\"\n" +
                "  ],\n" +
                "  \"unary\": [\n" +
                "    {\n" +
                "      \"lineitem\": [\n" +
                "        \"l_shipdate > DATE '1995-03-15'\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"orders\": [\n" +
                "        \"o_orderdate < DATE '1995-03-15'\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"customer\": [\n" +
                "        \"c_mktsegment = 'BUILDING'\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"relations\": [\n" +
                "    \"lineitem\",\n" +
                "    \"orders\",\n" +
                "    \"customer\"\n" +
                "  ]\n" +
                "}";

        SQLParser = new Parser(q3sql, TEST_GENERATED_JSON_PATH);
        SQLParser.parse();

        String actual_generated_json = FileUtils.readFileToString(new File(TEST_GENERATED_JSON_PATH), "UTF-8");
        assertEquals(expected_generated_json, actual_generated_json);

        String actual_information_json = FileUtils.readFileToString(new File(TEST_INFORMATION_JSON_PATH), "UTF-8");
        assertEquals(expected_information_json, actual_information_json);
    }

    @Test
    public void SQLParserQ6Test() throws Exception {
        String q6sql = "select\n" +
                "    sum(l_extendedprice*l_discount) as revenue\n" +
                "from \n" +
                "    lineitem\n" +
                "where \n" +
                "    l_shipdate >= date '1994-01-01'\n" +
                "    and l_shipdate < date '1995-01-01'\n" +
                "    and l_discount >= 0.05 \n" +
                "    and l_discount <= 0.07\n" +
                "    and l_quantity < 24;";
        String expected_generated_json = "{\n" +
                "  \"AggregateProcessFunction\": [\n" +
                "    {\n" +
                "      \"AggregateValue\": [\n" +
                "        {\n" +
                "          \"value_type\": \"Double\",\n" +
                "          \"name\": \"revenue\",\n" +
                "          \"aggregation\": \"+\",\n" +
                "          \"value\": {\n" +
                "            \"right_field\": {\n" +
                "              \"name\": \"l_discount\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_extendedprice\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"*\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"name\": \"QAggregate0\",\n" +
                "      \"delta_output\": true\n" +
                "    }\n" +
                "  ],\n" +
                "  \"join_structure\": [],\n" +
                "  \"RelationProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"orderkey\",\n" +
                "        \"l_linenumber\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Date\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"1994-01-01\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_shipdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \">=\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Date\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"1995-01-01\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_shipdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"<\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Double\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": 0.05\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_discount\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \">=\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Double\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": 0.07\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_discount\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"<=\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"int\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": 24\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_quantity\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"<\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qlineitem\",\n" +
                "      \"is_Root\": true,\n" +
                "      \"child_nodes\": 0,\n" +
                "      \"relation\": \"lineitem\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String expected_information_json = "{\n" +
                "  \"binary\": [],\n" +
                "  \"aggregation\": [\n" +
                "    \"revenue\"\n" +
                "  ],\n" +
                "  \"unary\": [\n" +
                "    {\n" +
                "      \"lineitem\": [\n" +
                "        \"l_shipdate >= DATE '1994-01-01'\",\n" +
                "        \"l_shipdate < DATE '1995-01-01'\",\n" +
                "        \"l_discount >= 0.05\",\n" +
                "        \"l_discount <= 0.07\",\n" +
                "        \"l_quantity < 24\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"relations\": [\n" +
                "    \"lineitem\"\n" +
                "  ]\n" +
                "}";

        SQLParser = new Parser(q6sql, TEST_GENERATED_JSON_PATH);
        SQLParser.parse();

        String actual_generated_json = FileUtils.readFileToString(new File(TEST_GENERATED_JSON_PATH), "UTF-8");
        assertEquals(expected_generated_json, actual_generated_json);

        String actual_information_json = FileUtils.readFileToString(new File(TEST_INFORMATION_JSON_PATH), "UTF-8");
        assertEquals(expected_information_json, actual_information_json);
    }

    @Test
    public void SQLParserQ4Test() throws Exception {
        String q4sql = "select o_orderpriority, count(distinct(l_orderkey)) as order_count\n" +
                "from lineitem, orders\n" +
                "where o_orderdate >= date '1993-07-01'\n" +
                "and o_orderdate < date '1993-10-01'\n" +
                "and l_commitdate < l_recriptdate\n" +
                "and l_orderkey = o_orderkey\n" +
                "group by o_orderpriority";
        String expected_generated_json = "";

        String expected_information_json = "{\n" +
                "  \"binary\": [],\n" +
                "  \"aggregation\": [\n" +
                "    \"revenue\"\n" +
                "  ],\n" +
                "  \"unary\": [\n" +
                "    {\n" +
                "      \"lineitem\": [\n" +
                "        \"l_shipdate >= DATE '1994-01-01'\",\n" +
                "        \"l_shipdate < DATE '1995-01-01'\",\n" +
                "        \"l_discount >= 0.05\",\n" +
                "        \"l_discount <= 0.07\",\n" +
                "        \"l_quantity < 24\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"relations\": [\n" +
                "    \"lineitem\"\n" +
                "  ]\n" +
                "}";

        SQLParser = new Parser(q4sql, TEST_GENERATED_JSON_PATH);
        SQLParser.parse();

        String actual_generated_json = FileUtils.readFileToString(new File(TEST_GENERATED_JSON_PATH), "UTF-8");
        assertEquals(expected_generated_json, actual_generated_json);

        String actual_information_json = FileUtils.readFileToString(new File(TEST_INFORMATION_JSON_PATH), "UTF-8");
        assertEquals(expected_information_json, actual_information_json);
    }

    @Test
    public void SQLParserQ10Test() throws Exception {
        String q10sql = "select\n" +
                "    c_custkey, \n" +
                "    c_name, \n" +
                "    sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
                "    c_acctbal, \n" +
                "    n_name, \n" +
                "    c_address, \n" +
                "    c_phone, \n" +
                "    c_comment\n" +
                "from \n" +
                "    customer, \n" +
                "    orders, \n" +
                "    lineitem, \n" +
                "    nation\n" +
                "where \n" +
                "    c_custkey = o_custkey\n" +
                "    and l_orderkey = o_orderkey\n" +
                "    and o_orderdate >= date '1993-10-01'\n" +
                "    and o_orderdate < date '1994-01-01'\n" +
                "    and l_returnflag = 'R'\n" +
                "    and c_nationkey = n_nationkey\n" +
                "group by \n" +
                "    c_custkey, \n" +
                "    c_name, \n" +
                "    c_acctbal, \n" +
                "    c_phone, \n" +
                "    n_name, \n" +
                "    c_address, \n" +
                "    c_comment;";
        String expected_generated_json = "{\n" +
                "  \"AggregateProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"custkey\",\n" +
                "        \"c_name\",\n" +
                "        \"c_acctbal\",\n" +
                "        \"c_phone\",\n" +
                "        \"n_name\",\n" +
                "        \"c_address\",\n" +
                "        \"c_comment\"\n" +
                "      ],\n" +
                "      \"AggregateValue\": [\n" +
                "        {\n" +
                "          \"value_type\": \"Double\",\n" +
                "          \"name\": \"revenue\",\n" +
                "          \"aggregation\": \"+\",\n" +
                "          \"value\": {\n" +
                "            \"right_field\": {\n" +
                "              \"right_field\": {\n" +
                "                \"name\": \"l_discount\",\n" +
                "                \"type\": \"attribute\",\n" +
                "                \"relation\": \"lineitem\"\n" +
                "              },\n" +
                "              \"left_field\": {\n" +
                "                \"var_type\": \"int\",\n" +
                "                \"type\": \"constant\",\n" +
                "                \"value\": 1\n" +
                "              },\n" +
                "              \"type\": \"expression\",\n" +
                "              \"operator\": \"-\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_extendedprice\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"*\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"output_key\": [\n" +
                "        \"custkey\",\n" +
                "        \"c_name\",\n" +
                "        \"c_acctbal\",\n" +
                "        \"c_phone\",\n" +
                "        \"n_name\",\n" +
                "        \"c_address\",\n" +
                "        \"c_comment\"\n" +
                "      ],\n" +
                "      \"name\": \"QAggregate0\",\n" +
                "      \"delta_output\": true\n" +
                "    }\n" +
                "  ],\n" +
                "  \"join_structure\": [\n" +
                "    {\n" +
                "      \"primary\": \"orders\",\n" +
                "      \"foreign\": \"lineitem\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"primary\": \"customer\",\n" +
                "      \"foreign\": \"orders\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"primary\": \"nation\",\n" +
                "      \"foreign\": \"customer\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"RelationProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"orderkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"varchar\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"R\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_returnflag\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"==\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qlineitem\",\n" +
                "      \"next_key\": [\n" +
                "        \"custkey\",\n" +
                "        \"c_name\",\n" +
                "        \"c_acctbal\",\n" +
                "        \"c_phone\",\n" +
                "        \"n_name\",\n" +
                "        \"c_address\",\n" +
                "        \"c_comment\"\n" +
                "      ],\n" +
                "      \"is_Root\": true,\n" +
                "      \"child_nodes\": 1,\n" +
                "      \"relation\": \"lineitem\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"nationkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"name\": \"Qnation\",\n" +
                "      \"next_key\": [\n" +
                "        \"nationkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 0,\n" +
                "      \"relation\": \"nation\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Date\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"1993-10-01\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"o_orderdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"orders\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \">=\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"Date\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"1994-01-01\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"o_orderdate\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"orders\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"<\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qorders\",\n" +
                "      \"next_key\": [\n" +
                "        \"orderkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 1,\n" +
                "      \"relation\": \"orders\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"nationkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"name\": \"Qcustomer\",\n" +
                "      \"next_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 1,\n" +
                "      \"relation\": \"customer\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String expected_information_json = "{\n" +
                "  \"binary\": [\n" +
                "    \"c_custkey = o_custkey\",\n" +
                "    \"l_orderkey = o_orderkey\",\n" +
                "    \"c_nationkey = n_nationkey\"\n" +
                "  ],\n" +
                "  \"aggregation\": [\n" +
                "    \"revenue\"\n" +
                "  ],\n" +
                "  \"unary\": [\n" +
                "    {\n" +
                "      \"lineitem\": [\n" +
                "        \"l_returnflag = 'R'\"\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"orders\": [\n" +
                "        \"o_orderdate >= DATE '1993-10-01'\",\n" +
                "        \"o_orderdate < DATE '1994-01-01'\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"relations\": [\n" +
                "    \"lineitem\",\n" +
                "    \"nation\",\n" +
                "    \"orders\",\n" +
                "    \"customer\"\n" +
                "  ]\n" +
                "}";

        SQLParser = new Parser(q10sql, TEST_GENERATED_JSON_PATH);
        SQLParser.parse();

        String actual_generated_json = FileUtils.readFileToString(new File(TEST_GENERATED_JSON_PATH), "UTF-8");
        assertEquals(expected_generated_json, actual_generated_json);

        String actual_information_json = FileUtils.readFileToString(new File(TEST_INFORMATION_JSON_PATH), "UTF-8");
        assertEquals(expected_information_json, actual_information_json);
    }

    @Test
    public void SQLParserQ12Test() throws Exception {
        String q12sql = "select\n" +
                "l_shipmode, \n" +
                "sum(case \n" +
                "when o_orderpriority ='1-URGENT'\n" +
                "or o_orderpriority ='2-HIGH'\n" +
                "then 1\n" +
                "else 0\n" +
                "end) as high_line_count,\n" +
                "sum(case \n" +
                "when o_orderpriority <> '1-URGENT'\n" +
                "and o_orderpriority <> '2-HIGH'\n" +
                "then 1\n" +
                "else 0\n" +
                "end) as low_line_count\n" +
                "from \n" +
                "orders, \n" +
                "lineitem\n" +
                "where \n" +
                "o_orderkey = l_orderkey\n" +
                "and (l_shipmode = 'MAIL' or l_shipmode =  'SHIP')\n" +
                "and l_commitdate < l_receiptdate\n" +
                "and l_shipdate < l_commitdate\n" +
                "and l_receiptdate >= date '1994-01-01'\n" +
                "and l_receiptdate < date '1995-01-01'\n" +
                "group by \n" +
                "l_shipmode;";
        String expected_generated_json = "";

        String expected_information_json = "";

        SQLParser = new Parser(q12sql, TEST_GENERATED_JSON_PATH);
        SQLParser.parse();

        String actual_generated_json = FileUtils.readFileToString(new File(TEST_GENERATED_JSON_PATH), "UTF-8");
        assertEquals(expected_generated_json, actual_generated_json);

        String actual_information_json = FileUtils.readFileToString(new File(TEST_INFORMATION_JSON_PATH), "UTF-8");
        assertEquals(expected_information_json, actual_information_json);
    }

    @Test
    public void SQLParserQ14Test() throws Exception {
        String q14sql = "select (100.00 * promosum / revenue) as promo_revenue\n" +
                "from \n" +
                "(\n" +
                "select sum(case when p_type like 'PROMO%'\n" +
                "           then l_extendedprice*(1-l_discount)\n" +
                "           else 0.0 \n" +
                "           end ) as promosum, \n" +
                "       sum(l_extendedprice * (1 - l_discount)) as revenue\n" +
                "from \n" +
                "lineitem,\n" +
                "part\n" +
                "where\n" +
                "l_partkey = p_partkey\n" +
                "and l_shipdate >= date '1995-09-01'\n" +
                "    and l_shipdate < date '1995-10-01'\n" +
                ") as T;";
        String expected_generated_json = "";

        String expected_information_json = "";

        SQLParser = new Parser(q14sql, TEST_GENERATED_JSON_PATH);
        SQLParser.parse();

        String actual_generated_json = FileUtils.readFileToString(new File(TEST_GENERATED_JSON_PATH), "UTF-8");
        assertEquals(expected_generated_json, actual_generated_json);

        String actual_information_json = FileUtils.readFileToString(new File(TEST_INFORMATION_JSON_PATH), "UTF-8");
        assertEquals(expected_information_json, actual_information_json);
    }

    @Test
    public void SQLParserQ18Test() throws Exception {
        String q18sql = "select \n" +
                "    * \n" +
                "from (\n" +
                "    select \n" +
                "        c_name,\n" +
                "        c_custkey, \n" +
                "        o_orderkey,\n" +
                "        o_orderdate,\n" +
                "        o_totalprice,\n" +
                "        sum(l_quantity) as CNT\n" +
                "    from \n" +
                "        customer,\n" +
                "        orders,\n" +
                "        lineitem\n" +
                "    where \n" +
                "        c_custkey = o_custkey\n" +
                "        and o_orderkey = l_orderkey\n" +
                "    group by \n" +
                "        c_name, \n" +
                "        c_custkey, \n" +
                "        o_orderkey, \n" +
                "        o_orderdate, \n" +
                "        o_totalprice) as T\n" +
                "where \n" +
                "    CNT > 300;";
        String expected_generated_json = "{\n" +
                "  \"AggregateProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"c_name\",\n" +
                "        \"custkey\",\n" +
                "        \"orderkey\",\n" +
                "        \"o_orderdate\",\n" +
                "        \"o_totalprice\"\n" +
                "      ],\n" +
                "      \"AggregateValue\": [\n" +
                "        {\n" +
                "          \"value_type\": \"Double\",\n" +
                "          \"name\": \"CNT\",\n" +
                "          \"aggregation\": \"+\",\n" +
                "          \"value\": {\n" +
                "            \"name\": \"l_quantity\",\n" +
                "            \"type\": \"attribute\",\n" +
                "            \"relation\": \"lineitem\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"output_key\": [\n" +
                "        \"c_name\",\n" +
                "        \"custkey\",\n" +
                "        \"orderkey\",\n" +
                "        \"o_orderdate\",\n" +
                "        \"o_totalprice\"\n" +
                "      ],\n" +
                "      \"name\": \"QAggregate0\",\n" +
                "      \"delta_output\": true,\n" +
                "      \"OutputSelectCondition\": [\n" +
                "        {\n" +
                "          \"right_field\": {\n" +
                "            \"var_type\": \"int\",\n" +
                "            \"type\": \"constant\",\n" +
                "            \"value\": 300\n" +
                "          },\n" +
                "          \"left_field\": {\n" +
                "            \"var_type\": \"Double\",\n" +
                "            \"name\": \"cnt\",\n" +
                "            \"store_type\": \"String\",\n" +
                "            \"type\": \"aggregate_attribute\"\n" +
                "          },\n" +
                "          \"type\": \"expression\",\n" +
                "          \"operator\": \">\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"join_structure\": [\n" +
                "    {\n" +
                "      \"primary\": \"orders\",\n" +
                "      \"foreign\": \"lineitem\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"primary\": \"customer\",\n" +
                "      \"foreign\": \"orders\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"RelationProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"orderkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"name\": \"Qlineitem\",\n" +
                "      \"next_key\": [\n" +
                "        \"c_name\",\n" +
                "        \"custkey\",\n" +
                "        \"orderkey\",\n" +
                "        \"o_orderdate\",\n" +
                "        \"o_totalprice\"\n" +
                "      ],\n" +
                "      \"is_Root\": true,\n" +
                "      \"child_nodes\": 1,\n" +
                "      \"relation\": \"lineitem\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"name\": \"Qorders\",\n" +
                "      \"next_key\": [\n" +
                "        \"orderkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 1,\n" +
                "      \"relation\": \"orders\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"name\": \"Qcustomer\",\n" +
                "      \"next_key\": [\n" +
                "        \"custkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 0,\n" +
                "      \"relation\": \"customer\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String expected_information_json = "{\n" +
                "  \"binary\": [\n" +
                "    \"c_custkey = o_custkey\",\n" +
                "    \"o_orderkey = l_orderkey\"\n" +
                "  ],\n" +
                "  \"aggregation\": [\n" +
                "    \"CNT\"\n" +
                "  ],\n" +
                "  \"unary\": [\n" +
                "    {\n" +
                "      \"aggregation\": [\n" +
                "        \"CNT > 300\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"relations\": [\n" +
                "    \"lineitem\",\n" +
                "    \"orders\",\n" +
                "    \"customer\"\n" +
                "  ]\n" +
                "}";

        SQLParser = new Parser(q18sql, TEST_GENERATED_JSON_PATH);
        SQLParser.parse();

        String actual_generated_json = FileUtils.readFileToString(new File(TEST_GENERATED_JSON_PATH), "UTF-8");
        assertEquals(expected_generated_json, actual_generated_json);

        String actual_information_json = FileUtils.readFileToString(new File(TEST_INFORMATION_JSON_PATH), "UTF-8");
        assertEquals(expected_information_json, actual_information_json);
    }

    @Test
    public void SQLParserQ19Test() throws Exception {
        String q19sql = "select\n" +
                "    sum(l_extendedprice * (1 - l_discount) ) as revenue\n" +
                "from \n" +
                "    lineitem, \n" +
                "    part\n" +
                "where\n" +
                "    p_partkey = l_partkey and \n" +
                "    (\n" +
                "        (\n" +
                "            p_brand = 'Brand#12'\n" +
                "            and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \n" +
                "            and l_quantity >= 1 and l_quantity <= 11\n" +
                "            and p_size >= 1 and p_size <= 5\n" +
                "        )\n" +
                "        or \n" +
                "        (\n" +
                "            p_brand = 'Brand#23'\n" +
                "            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n" +
                "            and l_quantity >= 10 and l_quantity <= 20\n" +
                "            and p_size >= 1 and p_size <= 10\n" +
                "        )\n" +
                "        or \n" +
                "        (\n" +
                "            p_brand = 'Brand#34'\n" +
                "            and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n" +
                "            and l_quantity >= 20 and l_quantity <= 30\n" +
                "            and p_size >= 1 and p_size <= 15\n" +
                "        )\n" +
                "    )\n" +
                "    and l_shipmode in ('AIR', 'AIR REG')\n" +
                "    and l_shipinstruct = 'DELIVER IN PERSON';";
        String expected_generated_json = "{\n" +
                "  \"AggregateProcessFunction\": [\n" +
                "    {\n" +
                "      \"AggregateValue\": [\n" +
                "        {\n" +
                "          \"value_type\": \"Double\",\n" +
                "          \"name\": \"revenue\",\n" +
                "          \"aggregation\": \"+\",\n" +
                "          \"value\": {\n" +
                "            \"right_field\": {\n" +
                "              \"right_field\": {\n" +
                "                \"name\": \"l_discount\",\n" +
                "                \"type\": \"attribute\",\n" +
                "                \"relation\": \"lineitem\"\n" +
                "              },\n" +
                "              \"left_field\": {\n" +
                "                \"var_type\": \"int\",\n" +
                "                \"type\": \"constant\",\n" +
                "                \"value\": 1\n" +
                "              },\n" +
                "              \"type\": \"expression\",\n" +
                "              \"operator\": \"-\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_extendedprice\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"*\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"name\": \"QAggregate0\",\n" +
                "      \"delta_output\": true\n" +
                "    }\n" +
                "  ],\n" +
                "  \"join_structure\": [],\n" +
                "  \"RelationProcessFunction\": [\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"orderkey\",\n" +
                "        \"l_linenumber\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"select_conditions\": {\n" +
                "        \"values\": [\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"right_field\": {\n" +
                "                \"right_field\": {\n" +
                "                  \"var_type\": \"int\",\n" +
                "                  \"type\": \"constant\",\n" +
                "                  \"value\": 15\n" +
                "                },\n" +
                "                \"left_field\": {\n" +
                "                  \"name\": \"p_size\",\n" +
                "                  \"type\": \"attribute\",\n" +
                "                  \"relation\": \"part\"\n" +
                "                },\n" +
                "                \"type\": \"expression\",\n" +
                "                \"operator\": \"<=\"\n" +
                "              },\n" +
                "              \"left_field\": {\n" +
                "                \"right_field\": {\n" +
                "                  \"right_field\": {\n" +
                "                    \"var_type\": \"int\",\n" +
                "                    \"type\": \"constant\",\n" +
                "                    \"value\": 1\n" +
                "                  },\n" +
                "                  \"left_field\": {\n" +
                "                    \"name\": \"p_size\",\n" +
                "                    \"type\": \"attribute\",\n" +
                "                    \"relation\": \"part\"\n" +
                "                  },\n" +
                "                  \"type\": \"expression\",\n" +
                "                  \"operator\": \">=\"\n" +
                "                },\n" +
                "                \"left_field\": {\n" +
                "                  \"right_field\": {\n" +
                "                    \"right_field\": {\n" +
                "                      \"var_type\": \"int\",\n" +
                "                      \"type\": \"constant\",\n" +
                "                      \"value\": 30\n" +
                "                    },\n" +
                "                    \"left_field\": {\n" +
                "                      \"name\": \"l_quantity\",\n" +
                "                      \"type\": \"attribute\",\n" +
                "                      \"relation\": \"lineitem\"\n" +
                "                    },\n" +
                "                    \"type\": \"expression\",\n" +
                "                    \"operator\": \"<=\"\n" +
                "                  },\n" +
                "                  \"left_field\": {\n" +
                "                    \"right_field\": {\n" +
                "                      \"right_field\": {\n" +
                "                        \"var_type\": \"int\",\n" +
                "                        \"type\": \"constant\",\n" +
                "                        \"value\": 20\n" +
                "                      },\n" +
                "                      \"left_field\": {\n" +
                "                        \"name\": \"l_quantity\",\n" +
                "                        \"type\": \"attribute\",\n" +
                "                        \"relation\": \"lineitem\"\n" +
                "                      },\n" +
                "                      \"type\": \"expression\",\n" +
                "                      \"operator\": \">=\"\n" +
                "                    },\n" +
                "                    \"left_field\": {\n" +
                "                      \"right_field\": {},\n" +
                "                      \"left_field\": {\n" +
                "                        \"right_field\": {\n" +
                "                          \"var_type\": \"varchar\",\n" +
                "                          \"type\": \"constant\",\n" +
                "                          \"value\": \"Brand#34\"\n" +
                "                        },\n" +
                "                        \"left_field\": {\n" +
                "                          \"name\": \"p_brand\",\n" +
                "                          \"type\": \"attribute\",\n" +
                "                          \"relation\": \"part\"\n" +
                "                        },\n" +
                "                        \"type\": \"expression\",\n" +
                "                        \"operator\": \"==\"\n" +
                "                      },\n" +
                "                      \"type\": \"expression\",\n" +
                "                      \"operator\": \"&&\"\n" +
                "                    },\n" +
                "                    \"type\": \"expression\",\n" +
                "                    \"operator\": \"&&\"\n" +
                "                  },\n" +
                "                  \"type\": \"expression\",\n" +
                "                  \"operator\": \"&&\"\n" +
                "                },\n" +
                "                \"type\": \"expression\",\n" +
                "                \"operator\": \"&&\"\n" +
                "              },\n" +
                "              \"type\": \"expression\",\n" +
                "              \"operator\": \"&&\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"right_field\": {\n" +
                "                \"right_field\": {\n" +
                "                  \"right_field\": {\n" +
                "                    \"var_type\": \"int\",\n" +
                "                    \"type\": \"constant\",\n" +
                "                    \"value\": 10\n" +
                "                  },\n" +
                "                  \"left_field\": {\n" +
                "                    \"name\": \"p_size\",\n" +
                "                    \"type\": \"attribute\",\n" +
                "                    \"relation\": \"part\"\n" +
                "                  },\n" +
                "                  \"type\": \"expression\",\n" +
                "                  \"operator\": \"<=\"\n" +
                "                },\n" +
                "                \"left_field\": {\n" +
                "                  \"right_field\": {\n" +
                "                    \"right_field\": {\n" +
                "                      \"var_type\": \"int\",\n" +
                "                      \"type\": \"constant\",\n" +
                "                      \"value\": 1\n" +
                "                    },\n" +
                "                    \"left_field\": {\n" +
                "                      \"name\": \"p_size\",\n" +
                "                      \"type\": \"attribute\",\n" +
                "                      \"relation\": \"part\"\n" +
                "                    },\n" +
                "                    \"type\": \"expression\",\n" +
                "                    \"operator\": \">=\"\n" +
                "                  },\n" +
                "                  \"left_field\": {\n" +
                "                    \"right_field\": {\n" +
                "                      \"right_field\": {\n" +
                "                        \"var_type\": \"int\",\n" +
                "                        \"type\": \"constant\",\n" +
                "                        \"value\": 20\n" +
                "                      },\n" +
                "                      \"left_field\": {\n" +
                "                        \"name\": \"l_quantity\",\n" +
                "                        \"type\": \"attribute\",\n" +
                "                        \"relation\": \"lineitem\"\n" +
                "                      },\n" +
                "                      \"type\": \"expression\",\n" +
                "                      \"operator\": \"<=\"\n" +
                "                    },\n" +
                "                    \"left_field\": {\n" +
                "                      \"right_field\": {\n" +
                "                        \"right_field\": {\n" +
                "                          \"var_type\": \"int\",\n" +
                "                          \"type\": \"constant\",\n" +
                "                          \"value\": 10\n" +
                "                        },\n" +
                "                        \"left_field\": {\n" +
                "                          \"name\": \"l_quantity\",\n" +
                "                          \"type\": \"attribute\",\n" +
                "                          \"relation\": \"lineitem\"\n" +
                "                        },\n" +
                "                        \"type\": \"expression\",\n" +
                "                        \"operator\": \">=\"\n" +
                "                      },\n" +
                "                      \"left_field\": {\n" +
                "                        \"right_field\": {},\n" +
                "                        \"left_field\": {\n" +
                "                          \"right_field\": {\n" +
                "                            \"var_type\": \"varchar\",\n" +
                "                            \"type\": \"constant\",\n" +
                "                            \"value\": \"Brand#23\"\n" +
                "                          },\n" +
                "                          \"left_field\": {\n" +
                "                            \"name\": \"p_brand\",\n" +
                "                            \"type\": \"attribute\",\n" +
                "                            \"relation\": \"part\"\n" +
                "                          },\n" +
                "                          \"type\": \"expression\",\n" +
                "                          \"operator\": \"==\"\n" +
                "                        },\n" +
                "                        \"type\": \"expression\",\n" +
                "                        \"operator\": \"&&\"\n" +
                "                      },\n" +
                "                      \"type\": \"expression\",\n" +
                "                      \"operator\": \"&&\"\n" +
                "                    },\n" +
                "                    \"type\": \"expression\",\n" +
                "                    \"operator\": \"&&\"\n" +
                "                  },\n" +
                "                  \"type\": \"expression\",\n" +
                "                  \"operator\": \"&&\"\n" +
                "                },\n" +
                "                \"type\": \"expression\",\n" +
                "                \"operator\": \"&&\"\n" +
                "              },\n" +
                "              \"left_field\": {\n" +
                "                \"right_field\": {\n" +
                "                  \"right_field\": {\n" +
                "                    \"var_type\": \"int\",\n" +
                "                    \"type\": \"constant\",\n" +
                "                    \"value\": 5\n" +
                "                  },\n" +
                "                  \"left_field\": {\n" +
                "                    \"name\": \"p_size\",\n" +
                "                    \"type\": \"attribute\",\n" +
                "                    \"relation\": \"part\"\n" +
                "                  },\n" +
                "                  \"type\": \"expression\",\n" +
                "                  \"operator\": \"<=\"\n" +
                "                },\n" +
                "                \"left_field\": {\n" +
                "                  \"right_field\": {\n" +
                "                    \"right_field\": {\n" +
                "                      \"var_type\": \"int\",\n" +
                "                      \"type\": \"constant\",\n" +
                "                      \"value\": 1\n" +
                "                    },\n" +
                "                    \"left_field\": {\n" +
                "                      \"name\": \"p_size\",\n" +
                "                      \"type\": \"attribute\",\n" +
                "                      \"relation\": \"part\"\n" +
                "                    },\n" +
                "                    \"type\": \"expression\",\n" +
                "                    \"operator\": \">=\"\n" +
                "                  },\n" +
                "                  \"left_field\": {\n" +
                "                    \"right_field\": {\n" +
                "                      \"right_field\": {\n" +
                "                        \"var_type\": \"int\",\n" +
                "                        \"type\": \"constant\",\n" +
                "                        \"value\": 11\n" +
                "                      },\n" +
                "                      \"left_field\": {\n" +
                "                        \"name\": \"l_quantity\",\n" +
                "                        \"type\": \"attribute\",\n" +
                "                        \"relation\": \"lineitem\"\n" +
                "                      },\n" +
                "                      \"type\": \"expression\",\n" +
                "                      \"operator\": \"<=\"\n" +
                "                    },\n" +
                "                    \"left_field\": {\n" +
                "                      \"right_field\": {\n" +
                "                        \"right_field\": {\n" +
                "                          \"var_type\": \"int\",\n" +
                "                          \"type\": \"constant\",\n" +
                "                          \"value\": 1\n" +
                "                        },\n" +
                "                        \"left_field\": {\n" +
                "                          \"name\": \"l_quantity\",\n" +
                "                          \"type\": \"attribute\",\n" +
                "                          \"relation\": \"lineitem\"\n" +
                "                        },\n" +
                "                        \"type\": \"expression\",\n" +
                "                        \"operator\": \">=\"\n" +
                "                      },\n" +
                "                      \"left_field\": {\n" +
                "                        \"right_field\": {},\n" +
                "                        \"left_field\": {\n" +
                "                          \"right_field\": {\n" +
                "                            \"var_type\": \"varchar\",\n" +
                "                            \"type\": \"constant\",\n" +
                "                            \"value\": \"Brand#12\"\n" +
                "                          },\n" +
                "                          \"left_field\": {\n" +
                "                            \"name\": \"p_brand\",\n" +
                "                            \"type\": \"attribute\",\n" +
                "                            \"relation\": \"part\"\n" +
                "                          },\n" +
                "                          \"type\": \"expression\",\n" +
                "                          \"operator\": \"==\"\n" +
                "                        },\n" +
                "                        \"type\": \"expression\",\n" +
                "                        \"operator\": \"&&\"\n" +
                "                      },\n" +
                "                      \"type\": \"expression\",\n" +
                "                      \"operator\": \"&&\"\n" +
                "                    },\n" +
                "                    \"type\": \"expression\",\n" +
                "                    \"operator\": \"&&\"\n" +
                "                  },\n" +
                "                  \"type\": \"expression\",\n" +
                "                  \"operator\": \"&&\"\n" +
                "                },\n" +
                "                \"type\": \"expression\",\n" +
                "                \"operator\": \"&&\"\n" +
                "              },\n" +
                "              \"type\": \"expression\",\n" +
                "              \"operator\": \"||\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"||\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"right_field\": {\n" +
                "                \"var_type\": \"varchar\",\n" +
                "                \"type\": \"constant\",\n" +
                "                \"value\": \"AIR REG\"\n" +
                "              },\n" +
                "              \"left_field\": {\n" +
                "                \"name\": \"l_shipmode\",\n" +
                "                \"type\": \"attribute\",\n" +
                "                \"relation\": \"lineitem\"\n" +
                "              },\n" +
                "              \"type\": \"expression\",\n" +
                "              \"operator\": \"==\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"right_field\": {\n" +
                "                \"var_type\": \"varchar\",\n" +
                "                \"type\": \"constant\",\n" +
                "                \"value\": \"AIR\"\n" +
                "              },\n" +
                "              \"left_field\": {\n" +
                "                \"name\": \"l_shipmode\",\n" +
                "                \"type\": \"attribute\",\n" +
                "                \"relation\": \"lineitem\"\n" +
                "              },\n" +
                "              \"type\": \"expression\",\n" +
                "              \"operator\": \"==\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"||\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"right_field\": {\n" +
                "              \"var_type\": \"varchar\",\n" +
                "              \"type\": \"constant\",\n" +
                "              \"value\": \"DELIVER IN PERSON\"\n" +
                "            },\n" +
                "            \"left_field\": {\n" +
                "              \"name\": \"l_shipinstruct\",\n" +
                "              \"type\": \"attribute\",\n" +
                "              \"relation\": \"lineitem\"\n" +
                "            },\n" +
                "            \"type\": \"expression\",\n" +
                "            \"operator\": \"==\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"operator\": \"&&\"\n" +
                "      },\n" +
                "      \"name\": \"Qlineitem\",\n" +
                "      \"is_Root\": true,\n" +
                "      \"child_nodes\": 0,\n" +
                "      \"relation\": \"lineitem\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"this_key\": [\n" +
                "        \"partkey\"\n" +
                "      ],\n" +
                "      \"is_Last\": true,\n" +
                "      \"name\": \"Qpart\",\n" +
                "      \"next_key\": [\n" +
                "        \"partkey\"\n" +
                "      ],\n" +
                "      \"is_Root\": false,\n" +
                "      \"child_nodes\": 0,\n" +
                "      \"relation\": \"part\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String expected_information_json = "{\n" +
                "  \"binary\": [\n" +
                "    \"p_partkey = l_partkey\"\n" +
                "  ],\n" +
                "  \"aggregation\": [\n" +
                "    \"revenue\"\n" +
                "  ],\n" +
                "  \"unary\": [\n" +
                "    {\n" +
                "      \"lineitem\": [\n" +
                "        \"(p_brand = 'Brand#12'\\n\\tAND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\\n\\tAND l_quantity >= 1\\n\\tAND l_quantity <= 11\\n\\tAND p_size >= 1\\n\\tAND p_size <= 5)\\nOR (p_brand = 'Brand#23'\\n\\tAND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\\n\\tAND l_quantity >= 10\\n\\tAND l_quantity <= 20\\n\\tAND p_size >= 1\\n\\tAND p_size <= 10)\\nOR (p_brand = 'Brand#34'\\n\\tAND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\\n\\tAND l_quantity >= 20\\n\\tAND l_quantity <= 30\\n\\tAND p_size >= 1\\n\\tAND p_size <= 15)\",\n" +
                "        \"l_shipmode IN ('AIR', 'AIR REG')\",\n" +
                "        \"l_shipinstruct = 'DELIVER IN PERSON'\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"relations\": [\n" +
                "    \"lineitem\",\n" +
                "    \"part\"\n" +
                "  ]\n" +
                "}";

        SQLParser = new Parser(q19sql, TEST_GENERATED_JSON_PATH);
        SQLParser.parse();

        String actual_generated_json = FileUtils.readFileToString(new File(TEST_GENERATED_JSON_PATH), "UTF-8");
        assertEquals(expected_generated_json, actual_generated_json);

        String actual_information_json = FileUtils.readFileToString(new File(TEST_INFORMATION_JSON_PATH), "UTF-8");
        assertEquals(expected_information_json, actual_information_json);
    }

    @After
    public void clean_garbage() {
        deleteFile(TEST_GENERATED_JSON_PATH);
        deleteFile(TEST_INFORMATION_JSON_PATH);
    }

    private boolean deleteFile(String path) {
        File file = new File(path);
        if (file.exists()) {
            return file.delete();
        }
        else {
            return false;
        }
    }
}
