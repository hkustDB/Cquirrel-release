import React, {Component} from 'react';
import {io} from 'socket.io-client';
import InstructSteps from './components/InstructSteps'
import SqlEntry from './components/SqlEntry';
import JsonFileUploader from './components/JsonFileUploader';
import CodegenResult from "./components/CodegenResult";
import FlowDiag from "./components/FlowDiag";
import QueryTable from "./components/QueryTable";
// import QueryFig from "./components/QueryFig"
import Settings from "./components/Settings";
import About from "./components/About";
import SqlEditor from "./components/SqlEditor";
import RelationGraph from "./components/RelationGraph"
import axios from 'axios';
import {Layout, Menu, Row, Col, message, Card, Button, Input, InputNumber, Divider, Spin} from 'antd';
import {PlayCircleOutlined, PauseCircleOutlined} from '@ant-design/icons';
import './App.css';

import echarts from "echarts/lib/echarts";
import 'echarts/lib/chart/line';
import 'echarts/lib/chart/bar';
import 'echarts/lib/component/tooltip';
import 'echarts/lib/component/title';
import 'echarts/lib/component/legend';
import 'echarts/lib/component/legendScroll';
import 'echarts/lib/component/grid';
import 'echarts/lib/component/dataZoom';
import {consoleLog} from "echarts/lib/util/log";


class App extends Component {

    constructor(props) {
        super(props);
        // var myChart = {};
        this.state = {
            socket: null,
            connected: false,
            codegen_log: "",
            cur_step: 0,
            table_cols: [],
            table_data: [],
            table_title: "Query Result Table: ",
            topN_input_disabled: false,
            aggregate_name_input_disabled: false,
            aggregate_name: 'revenue',
            showRelationGraph: false,
            relationsData: {},
            showFlowDiag: false,
            flowDiagData: {},
            queryChartLoading: false,
            queryTableLoading: false,
            codegenLogLoading: false,
            flowDiagLoading: false,
            relationGraphLoading: false,

            chart_option: {
                animation: false,
                title: {
                    text: 'Result Chart'
                },
                tooltip: {},
                legend: {
                    type: 'scroll',
                    orient: 'vertical',
                    left: '75%',
                    top: '10%',
                    bottom: '0%',
                    textStyle: {
                        fontFamily: 'Monaco',
                        fontSize: 10,
                        width: '25%'
                    },
                    formatter: function (params) {
                        let tip1 = "";
                        let tip = "";
                        let le = params.length
                        let num = 40;
                        if (le > num) {
                            let l = Math.ceil(le / num);
                            for (let i = 1; i <= l; i++) {
                                if (i < l) {
                                    tip1 += params.slice(i * num - num, i * num) + '\n';
                                } else if (i === l) {
                                    tip = tip1 + params.slice((l - 1) * num, le);
                                }
                            }
                            return tip;
                        } else {
                            return params;
                        }
                    },
                    data: []
                },
                grid: {
                    left: "5%",
                    bottom: "12%",
                    right: '25%',
                    show: true
                },
                dataZoom: {
                    type: "slider",
                    show: true,
                    showDetail: true,
                    realtime: true,
                    // bottom: '23%'
                },
                xAxis: {
                    type: 'category',
                    name: 'timestamp',
                    data: []
                },
                yAxis: {
                    type: 'value',
                    axisLabel: {
                        inside: true
                    },
                    name: "aggregate"
                },
                series: []
            },
        };
    }

    componentDidMount() {
        this.build();
        window.addEventListener('beforeunload', this.beforeUnload);
    }

    componentWillUnmount() {
        window.removeEventListener('beforeunload', this.beforeUnload);
        this.disconnect();
    }

    beforeUnload = e => {
        this.disconnect();
    };

    build = () => {
        // init echarts
        var myChart = echarts.init(document.getElementById('queryChart'));
        myChart.setOption(this.state.chart_option);

        // var local_data = [];
        var x_timestamp = [];
        var legend_data = [];
        // var selected_data = {};
        // var selected_queue = new Queue();
        var q6_serie = {name: "revenue", type: "line", data: []};

        // init websocket
        const _socket = io('http://localhost:5000/ws', {
            transports: ['websocket']
        });
        // const _socket = io('http://localhost:5000/ws', {
        //     transports: ['polling']
        // });
        _socket.on('connect', () => {
            console.log("socket connect.");
            this.setState({connected: true});
        });
        _socket.on('disconnect', () => {
            console.log("socket disconnect.")
            this.setState({connected: false});
        });
        _socket.on('r_codegen_log', data => {
            this.setState({
                codegenLogLoading: false,
            })
            this.setState({codegen_log: data.codegen_log}, () => {
                console.log("r_codegen_log: ", this.state.codegen_log)
            });
            if (data.retcode === 0) {
                this.setState({cur_step: 3}, () => {
                    console.log("cur_step: ", this.state.cur_step)
                });
            } else {
                this.setState({cur_step: 1}, () => {
                    console.log("cur_step: ", this.state.cur_step)
                });
            }
        });

        _socket.on('r_set_step', data => {
            console.log("received r_set_step signal: ", data)
            this.setState({cur_step: data.step}, () => {
                console.log("r_set_step:", this.state.cur_step)
            });
        });

        _socket.on('r_start_to_send_data', data => {

            if (data.status === "start") {
                // local_data = [];
                x_timestamp = [];
                legend_data = [];
                q6_serie = {name: "revenue", type: "line", data: []};

                this.setState({
                    table_cols: [],
                    table_data: [],
                })
            }

        });

        _socket.on('r_information_data', res => {
            console.log("information json: ")
            // console.log(res)
            // console.log(typeof res.information_data)
            console.log(JSON.parse(res.information_data))
            console.log(JSON.parse(res.information_data).relations)
            this.setState({
                relationsData: JSON.parse(res.information_data),
                relationGraphLoading: false,
                showRelationGraph: true,

                flowDiagData: JSON.parse(res.information_data).relations,
                flowDiagLoading: false,
                showFlowDiag: true,
            }, () => {
                console.log("relations data:")
                console.log(this.state.relationsData)
            })
        })

        _socket.on('r_figure_data', res => {

            this.setState({
                queryTableLoading: false,
                queryChartLoading: false,
                topN_input_disabled: true,
                aggregate_name_input_disabled: true
            })

            // console.log("r_figure_data: ", res)
            if (res.isTopN === 0) {
                // refresh table
                let t_cols = [
                    {
                        title: 'timestamp',
                        dataIndex: 'timestamp',
                    },
                    {
                        title: 'revenue',
                        dataIndex: 'revenue',
                    },
                ]
                // let t_data = [...this.state.table_data];
                let t_data = [{
                    key: (this.state.table_data.length + 1).toString(),
                    timestamp: res.data[2],
                    revenue: res.data[0],
                }, ...this.state.table_data];
                let t_title = "TPC-H Query Result Table: "

                this.setState({table_cols: t_cols, table_data: t_data, table_title: t_title});

                // refresh chart
                let line_list = res.data
                let c_option = {...this.state.chart_option};
                c_option.title.text = "Result Chart - TPC-H Query";
                q6_serie.data.push(line_list[0]);
                c_option.xAxis.data.push(line_list[2]);
                c_option.series[0] = q6_serie;
                let q6_total_length = c_option.xAxis.data.length;
                c_option.dataZoom.startValue = ((q6_total_length - 100) > 0) ? q6_total_length - 100 : 1;
                myChart.setOption(c_option);
                this.setState({chart_option: c_option});
            } else if (res.isTopN === 1) {
                let top_value_data = res.top_value_data;
                console.log(res);
                let line_list = res.data
                let line_list_len = line_list.length;
                let attribute_length = ((line_list_len - 1) / 2);
                // refresh table

                // construct column name
                let t_cols = [
                    {
                        title: 'timestamp',
                        dataIndex: 'timestamp',
                    },
                ]
                for (var i = 0; i < attribute_length; i++) {
                    let tmp_col = {
                        title: line_list[attribute_length + i],
                        dataIndex: line_list[attribute_length + i],
                    }
                    t_cols.push(tmp_col);
                }

                // construct table data index
                let tmp_t_data = {
                    key: (this.state.table_data.length + 1).toString(),
                }
                for (var i = 0; i < attribute_length; i++) {
                    tmp_t_data[line_list[attribute_length + i]] = line_list[i];
                    tmp_t_data["timestamp"] = line_list[line_list_len - 1];
                }
                let t_data = [tmp_t_data, ...this.state.table_data];
                let t_title = "TPC-H Query Result Table: "

                this.setState({table_cols: t_cols, table_data: t_data, table_title: t_title});

                // refresh chart
                // let line_list = res.data
                let c_option = {...this.state.chart_option};
                x_timestamp = res.x_timestamp;


                c_option.title.text = "Result Chart - TPC-H Query  -- Top " + Object.keys(top_value_data).length;
                legend_data = []
                var series_data = []

                for (var key_tag in top_value_data) {
                    legend_data.push(key_tag);
                    let aserie = {name: key_tag, type: "line", data: top_value_data[key_tag]};
                    series_data.push(aserie);
                }
                // option.series.push(aserie);
                c_option.series = series_data;
                c_option.legend.data = legend_data;
                c_option.xAxis.data = x_timestamp;
                c_option.dataZoom.startValue = ((x_timestamp.length - 100) > 0) ? x_timestamp.length - 100 : 1;
                myChart.setOption(c_option);
                this.setState({chart_option: c_option});
            } else {
                console.log('query number is not supported yet.')
            }
        });

        _socket.on("r_message", data => {
            if (data.m_type === "info") {
                message.info(data.message);
            } else if (data.m_type === "success") {
                message.success(data.message);
            } else if (data.m_type === "error") {
                message.error(data.message);
            } else if (data.m_type === "warning") {
                message.warning(data.message);
            } else {
                console.log("unknow message type", data)
            }
        });

        this.setState({socket: _socket}, () => {
            let {socket, connected} = this.state;
            if (!connected) {
                socket.connect();
            }
        });
    };

    disconnect = () => {
        try {
            let {socket, connected} = this.state;
            if (null != socket && connected) {
                socket.disconnect();
            }
        } catch (error) {
            console.log(error);
        }
    };

    send = () => {
        try {
            let {socket, connected} = this.state;
            if (null != socket && connected) {
                // send message to server
                socket.emit('topic', '新消息来啦~');
            }
        } catch (error) {
            console.log(error);
        }
    };

    stopServerSendDataThread = () => {
        let _socket = this.state.socket;
        _socket.emit("r_stop_send_data_thread", {"status": "stop"})
    }

    pauseServerSendData = () => {
        let _socket = this.state.socket;
        _socket.emit("r_send_data_control", {"command": "pause"})
        this.setState({
            topN_input_disabled: false,
            aggregate_name_input_disabled: false
        })
        console.log("pause server send data")
    }

    restartServerSendData = () => {
        let _socket = this.state.socket;
        _socket.emit("r_send_data_control", {"command": "restart"})
        this.setState({
            topN_input_disabled: true,
            aggregate_name_input_disabled: true
        })
        console.log("restart server send data")
    }

    set_topN_value = () => {

    }

    set_aggregate_name = (e) => {
        console.log("set_aggregate_name: ", e.target.value)
        this.setState({
            aggregate_name: e.target.value,
            chart_option: {
                animation: false,
                title: {
                    text: 'Result Chart'
                },
                tooltip: {},
                legend: {
                    type: 'scroll',
                    orient: 'vertical',
                    top: '78%',
                    bottom: '0%',
                    textStyle: {
                        fontSize: 10
                    },
                    data: []
                },
                grid: {
                    left: "5%",
                    bottom: "32%",
                    show: true
                },
                dataZoom: {
                    type: "slider",
                    show: true,
                    showDetail: true,
                    realtime: true,
                    bottom: '23%'
                },
                xAxis: {
                    type: 'category',
                    name: 'timestamp',
                    data: []
                },
                yAxis: {
                    type: 'value',
                    axisLabel: {
                        inside: true
                    },
                    name: e.target.value,
                },
                series: []
            },
        })
    }

    handleSubmitSql = (v) => {

        function wait(ms) {
            var start = new Date().getTime();
            var end = start;
            while (end < start + ms) {
                end = new Date().getTime();
            }
        }

        // console.log(v)
        // this.setState({
        //     relationGraphLoading: true,
        //     flowDiagLoading: true,
        // }, () => {
        //     wait(1000);
        // })
        //
        // wait(1000);
        // this.setState({
        //     // relationGraphLoading: false,
        //     flowDiagLoading: false,
        // }, () => {
        //     wait(1000);
        // })
        // wait(500);

        this.setState({
            // showRelationGraph: true,
            // showFlowDiag: true,

            relationGraphLoading: true,
            flowDiagLoading: true,
            codegenLogLoading: true,
            queryTableLoading: true,
            queryChartLoading: true,
        })



        axios.post('http://localhost:5000/r/submit_sql', {sql: v}).then(res => {
            console.log(res);
        })
    }


    render() {
        const {Header, Content, Footer} = Layout;
        return (
            <div className="App">
                <Layout>
                    <Header style={{position: 'fixed', zIndex: 1, width: '100%'}}>
                        <div className="logo"/>
                        <Menu theme="dark" mode="horizontal" defaultSelectedKeys={['1']}>
                            <Menu.Item key="1"><a href="/"
                                                  onClick={this.stopServerSendDataThread.bind(this)}>Cquirrel </a>
                            </Menu.Item>
                            <Menu.Item key="2" style={{float: 'right'}}><Settings/></Menu.Item>
                            <Menu.Item key="3" style={{float: 'right'}}><About/></Menu.Item>
                        </Menu>
                    </Header>

                    {/*<Layout>*/}
                    {/*    <Sider width={200} style={{backgroundColor:'white'}}>*/}

                    {/*    </Sider>*/}
                    <Layout>
                        <Content className="site-layout" style={{padding: '0 50px', marginTop: 64}}>
                            <div className="site-layout-background" style={{padding: 24, minHeight: 640}}>
                                <Row>
                                    <InstructSteps cur_step={this.state.cur_step}/>
                                </Row>

                                {/*<Row>*/}
                                {/*    <Col span={24}><SqlEditor/></Col>*/}
                                {/*</Row>*/}

                                <Divider/>
                                <Row>
                                    <Col span={6}>
                                        <Row>
                                            <Col span={24}>
                                                <SqlEntry onSubmitSql={this.handleSubmitSql}/>
                                            </Col>
                                        </Row>
                                        <Row>
                                            <Col span={24}>
                                                <Spin tip={"loading"} spinning={this.state.relationGraphLoading}>
                                                    <RelationGraph showRelationGraph={this.state.showRelationGraph}
                                                                   relationsData={this.state.relationsData}/>
                                                </Spin>
                                            </Col>
                                        </Row>
                                        <Divider/>
                                        <Row>
                                            <Col span={24}><span>Or you can upload the json file:</span>
                                                <JsonFileUploader/></Col>
                                        </Row>
                                    </Col>

                                    <Col span={18}>
                                        <Row>
                                            <Col span={12}>
                                                <Spin tip={"loading"} spinning={this.state.flowDiagLoading}>
                                                    <FlowDiag showFlowDiag={this.state.showFlowDiag}
                                                              flowDiagData={this.state.flowDiagData}/>
                                                </Spin>
                                            </Col>
                                            <Col span={12}>
                                                <Spin tip={"loading"} spinning={this.state.codegenLogLoading}>
                                                    <CodegenResult logContent={this.state.codegen_log}/>
                                                </Spin>
                                            </Col>
                                        </Row>
                                        <Divider/>
                                        <Row>
                                            <Col span={24}>
                                                <Spin tip={"loading"} spinning={this.state.queryTableLoading}>
                                                    <QueryTable table_cols={this.state.table_cols}
                                                                table_data={this.state.table_data}
                                                                table_title={this.state.table_title}/>
                                                </Spin>
                                            </Col>
                                        </Row>
                                        <Divider/>
                                        <Row>
                                            <Col span={24}>
                                                {/*<QueryFig/>*/}
                                                <Spin tip={"Loading"} spinning={this.state.queryChartLoading}>
                                                    <div className="queryfig-card">

                                                        <Card title="Query Result Figure: " extra={
                                                            <div>
                                                                <Input.Group size="small" compact>
                                                                    Aggregate Name:&nbsp;
                                                                    <Input defaultValue="" size="small"
                                                                           style={{width: 'min-content'}}
                                                                           disabled={this.state.aggregate_name_input_disabled}
                                                                           onPressEnter={this.set_aggregate_name.bind(this)}/>
                                                                    &nbsp;&nbsp;
                                                                    TopN:&nbsp;
                                                                    <InputNumber defaultValue={10} size="small"
                                                                                 disabled={this.state.topN_input_disabled}
                                                                                 onPressEnter={this.set_topN_value.bind(this)}/>
                                                                    &nbsp;&nbsp;&nbsp;
                                                                    <Button type="primary" size="small"
                                                                            icon={<PlayCircleOutlined/>}
                                                                            title="start to send data"
                                                                            onClick={this.restartServerSendData.bind(this)}/>
                                                                    &nbsp;&nbsp;
                                                                    <Button type="primary" size="small"
                                                                            icon={<PauseCircleOutlined/>}
                                                                            title="stop to send data"
                                                                            onClick={this.pauseServerSendData.bind(this)}/>
                                                                </Input.Group>
                                                            </div>}>

                                                            <div id="queryChart"
                                                                 style={{width: "100%", height: 650}}></div>

                                                        </Card>

                                                    </div>
                                                </Spin>
                                            </Col>
                                        </Row>
                                    </Col>
                                </Row>

                            </div>
                        </Content>
                        <Footer style={{textAlign: 'center'}}>HKUST & Alibaba</Footer>
                    </Layout>
                </Layout>
                {/*</Layout>*/}

            </div>
        );
    }

}

export default App;
