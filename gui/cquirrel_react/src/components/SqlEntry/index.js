import React, {Component} from 'react'
import MonacoEditor from 'react-monaco-editor';
import {Input, Card, Button, Divider} from 'antd'
import SqlEditor from "../SqlEditor";

import "./sqlentry-card.css"
import axios from "axios";

export default class SqlEntry extends Component {
    constructor(props) {
        super(props);
        this.state = {
            code: '',
        }
    }

    editorDidMount(editor, monaco) {
        console.log('editorDidMount', editor);
        editor.focus();
    }

    onChange(newValue, e) {
        console.log('onChange', newValue, e);
    }

    setEditorCodeAsQ1 = () => {
        this.setState({
            code: "select\n" +
                "    l_returnflag, \n" +
                "    l_linestatus, \n" +
                "    sum(l_quantity) as sum_qty,\n" +
                "    sum(l_extendedprice) as sum_base_price,\n" +
                "    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,\n" +
                "    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,\n" +
                "    avg(l_quantity) as avg_qty, \n" +
                "    avg(l_extendedprice) as avg_price,\n" +
                "    avg(l_discount) as avg_disc, \n" +
                "    count(*) as count_order\n" +
                "from \n" +
                "    lineitem \n" +
                "where \n" +
                "    l_shipdate <= date '1998-09-02' \n" +
                "group by \n" +
                "    l_returnflag, \n" +
                "    l_linestatus;"
        })
    }

    setEditorCodeAsQ3 = () => {
        this.setState({
            code: "select\n" +
                "    l_orderkey, \n" +
                "    sum(l_extendedprice*(1-l_discount)) as revenue,\n" +
                "    o_orderdate, \n" +
                "    o_shippriority\n" +
                "from \n" +
                "    customer, \n" +
                "    orders, \n" +
                "    lineitem\n" +
                "where \n" +
                "    c_mktsegment = 'BUILDING'\n" +
                "    and c_custkey = o_custkey\n" +
                "    and l_orderkey = o_orderkey\n" +
                "    and o_orderdate < date '1995-03-15'\n" +
                "    and l_shipdate > date '1995-03-15'\n" +
                "group by \n" +
                "    l_orderkey, \n" +
                "    o_orderdate, \n" +
                "    o_shippriority"
        })
    }

    setEditorCodeAsQ4 = () => {
        this.setState({
            code: "select \n" +
                "    o_orderpriority, \n" +
                "    count(distinct(l_orderkey)) as order_count\n" +
                "from \n" +
                "    lineitem, \n" +
                "    orders\n" +
                "where \n" +
                "    o_orderdate >= date '1993-07-01'\n" +
                "    and o_orderdate < date '1993-10-01'\n" +
                "    and l_commitdate < l_receiptdate\n" +
                "    and l_orderkey = o_orderkey\n" +
                "group by \n" +
                "    o_orderpriority"
        })
    }

    setEditorCodeAsQ5 = () => {
        this.setState({
            code: "select\n" +
                "    n_name, \n" +
                "    sum(l_extendedprice * (1 - l_discount)) as revenue\n" +
                "from \n" +
                "    customer, \n" +
                "    orders, \n" +
                "    lineitem, \n" +
                "    supplier, \n" +
                "    nation, \n" +
                "    region\n" +
                "where \n" +
                "    c_custkey = o_custkey\n" +
                "    and l_orderkey = o_orderkey\n" +
                "    and l_suppkey = s_suppkey\n" +
                "    and c_nationkey = s_nationkey\n" +
                "    and s_nationkey = n_nationkey\n" +
                "    and n_regionkey = r_regionkey\n" +
                "    and r_name = '[REGION]'\n" +
                "    and o_orderdate >= date '1994-01-01'\n" +
                "    and o_orderdate < date '1995-01-01'\n" +
                "group by \n" +
                "    n_name;"
        })
    }

    setEditorCodeAsQ6 = () => {
        this.setState({
            code: "select\n" +
                "    sum(l_extendedprice*l_discount) as revenue\n" +
                "from \n" +
                "    lineitem\n" +
                "where \n" +
                "    l_shipdate >= date '1994-01-01'\n" +
                "    and l_shipdate < date '1995-01-01'\n" +
                "    and l_discount >= 0.05 \n" +
                "    and l_discount <= 0.07\n" +
                "    and l_quantity < 24;"
        })
    }

    setEditorCodeAsQ7 = () => {
        this.setState({
            code: "select \n" +
                "    n1.n_name as supp_nation, \n" +
                "    n2.n_name as cust_nation, \n" +
                "    l_year,\n" +
                "    l_extendedprice * (1 - l_discount) as volume\n" +
                "from \n" +
                "    supplier, \n" +
                "    lineitem, \n" +
                "    orders, \n" +
                "    customer, \n" +
                "    nation n1, \n" +
                "    nation n2\n" +
                "where \n" +
                "    s_suppkey = l_suppkey\n" +
                "    and o_orderkey = l_orderkey\n" +
                "    and c_custkey = o_custkey\n" +
                "    and s_nationkey = n1.n_nationkey\n" +
                "    and c_nationkey = n2.n_nationkey\n" +
                "    and (n1.n_name = 'FRANCE' or n1.n_name = 'GERMANY')\n" +
                "    and (n2.n_name = 'FRANCE' or n2.n_name = 'GERMANY')\n" +
                "    and n1.n_name <> n2.n_name\n" +
                "    and l_shipdate >= date '1995-01-01' \n" +
                "    and l_shipdate <= date '1996-12-31'\n" +
                "group by\n" +
                "    supp_nation,\n" +
                "    cust_nation,\n" +
                "    l_year;"
        })
    }

    setEditorCodeAsQ9 = () => {
        this.setState({
            code: "select \n" +
                "    n_name, \n" +
                "    o_year,\n" +
                "    sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as amount\n" +
                "from \n" +
                "    part, \n" +
                "    supplier, \n" +
                "    lineitem, \n" +
                "    partsupp, \n" +
                "    orders, \n" +
                "    nation\n" +
                "where \n" +
                "    s_suppkey = l_suppkey\n" +
                "    and ps_suppkey = l_suppkey\n" +
                "    and ps_partkey = l_partkey\n" +
                "    and p_partkey = l_partkey\n" +
                "    and o_orderkey = l_orderkey\n" +
                "    and s_nationkey = n_nationkey\n" +
                "    and p_name like '%green%'\n" +
                "group by\n" +
                "    n_name,\n" +
                "    o_year;"
        })
    }

    setEditorCodeAsQ10 = () => {
        this.setState({
            code: "select\n" +
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
                "    c_comment;"
        })
    }

    setEditorCodeAsQ12 = () => {
        this.setState({
            code: "select\n" +
                "    l_shipmode, \n" +
                "    sum(case \n" +
                "            when o_orderpriority ='1-URGENT'\n" +
                "                or o_orderpriority ='2-HIGH'\n" +
                "            then 1\n" +
                "            else 0\n" +
                "    end) as high_line_count,\n" +
                "    sum(case \n" +
                "            when o_orderpriority <> '1-URGENT'\n" +
                "                and o_orderpriority <> '2-HIGH'\n" +
                "            then 1\n" +
                "            else 0\n" +
                "    end) as low_line_count\n" +
                "from \n" +
                "    orders, \n" +
                "    lineitem\n" +
                "where \n" +
                "    o_orderkey = l_orderkey\n" +
                "    and (l_shipmode = 'MAIL' or l_shipmode =  'SHIP')\n" +
                "    and l_commitdate < l_receiptdate\n" +
                "    and l_shipdate < l_commitdate\n" +
                "    and l_receiptdate >= date '1994-01-01'\n" +
                "    and l_receiptdate < date '1995-01-01'\n" +
                "group by \n" +
                "    l_shipmode;"
        })
    }

    setEditorCodeAsQ14 = () => {
        this.setState({
            code: "select (100.00 * promosum / revenue) as promo_revenue\n" +
                "from \n" +
                "(\n" +
                "    select sum(case when p_type like 'PROMO%'\n" +
                "            then l_extendedprice * (1 - l_discount)\n" +
                "            else 0.0 \n" +
                "            end ) as promosum, \n" +
                "        sum(l_extendedprice * (1 - l_discount)) as revenue\n" +
                "    from \n" +
                "        lineitem,\n" +
                "        part\n" +
                "    where\n" +
                "        l_partkey = p_partkey\n" +
                "        and l_shipdate >= date '1995-09-01'\n" +
                "        and l_shipdate < date '1995-10-01'\n" +
                ") as T;"
        })
    }

    setEditorCodeAsQ16 = () => {
        this.setState({
            code: "select\n" +
                "    p_brand, \n" +
                "    p_type, \n" +
                "    p_size, \n" +
                "    count(distinct ps_suppkey) as supplier_cnt\n" +
                "from \n" +
                "    partsupp, \n" +
                "    part,\n" +
                "    supplier\n" +
                "where \n" +
                "    p_partkey = ps_partkey\n" +
                "    and p_brand <> 'Brand#45'\n" +
                "    and p_type not like 'MEDIUM POLISHED%'\n" +
                "    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)\n" +
                "    and ps_suppkey = s_suppkey\n" +
                "    and s_comment not like '%Customer%Complaints%'\n" +
                "group by \n" +
                "    p_brand, \n" +
                "    p_type, \n" +
                "    p_size;"
        })
    }

    setEditorCodeAsQ18 = () => {
        this.setState({
            code: "select \n" +
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
                "    CNT > 300;"
        })
    }

    setEditorCodeAsQ19 = () => {
        this.setState({
            code: "select\n" +
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
                "    and l_shipinstruct = 'DELIVER IN PERSON';"
        })
    }

    setEditorCodeAsNull = () => {
        this.setState({
            code: ""
        })
    }

    handleSubmitSql = () => {
        this.props.onSubmitSql(this.refs.monaco.editor.getValue())
    }

    render() {

        const {TextArea} = Input;

        const options = {
            selectOnLineNumbers: true,
            automaticLayout: true,
            formatOnPaste: true,
            lineNumbers: true,
            minimap: {
                enabled: false
            }
        };

        return (
            <div className="sqlentry-card">
                <Card title="Input SQL:" extra={<Button size="small" type="primary" onClick={this.handleSubmitSql}>Submit SQL</Button>}>
                    {/*<TextArea placeholder="Please enter SQL here." showCount></TextArea>*/}
                    <div style={{textAlign: "left"}}>
                        <MonacoEditor
                            width="100%"
                            height="400"
                            language="sql"
                            theme="vs-light"

                            value={this.state.code}
                            options={options}
                            onChange={this.onChange}
                            editorDidMount={this.editorDidMount}
                            ref="monaco"
                        />
                    </div>
                    <Divider/>
                    <div style={{textAlign: "left"}}>
                        <span>TPC-H Query Templates:  </span>
                        <Button shape="round" onClick={this.setEditorCodeAsQ1}>Q1</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ3}>Q3</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ4}>Q4</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ5}>Q5</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ6}>Q6</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ7}>Q7</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ9}>Q9</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ10}>Q10</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ12}>Q12</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ14}>Q14</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ16}>Q16</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ18}>Q18</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsQ19}>Q19</Button>
                        <Button shape="round" onClick={this.setEditorCodeAsNull}>Clear</Button>
                    </div>
                </Card>

            </div>
        );
    }
}
