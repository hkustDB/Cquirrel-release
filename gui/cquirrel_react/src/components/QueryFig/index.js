import React, {Component} from 'react';
import {Card} from 'antd';

import QueryChart from "../QueryChart"
import "./queryfig-card.css"
import echarts from "echarts/lib/echarts";
import 'echarts/lib/chart/bar';
import 'echarts/lib/component/tooltip';
import 'echarts/lib/component/title';
import 'echarts/lib/component/legend';
import 'echarts/lib/component/grid';
import 'echarts/lib/component/dataZoom';
import 'echarts/lib/chart/line';

export default class QueryFig extends Component {

    static defaultProps = {
        myOption: {
            animation: false,
            title: {
                text: 'AJU Result Chart'
            },
            tooltip: {},
            legend: {
                type: 'scroll',
                orient: 'vertical',
                left: '80%',
                right: '0%',
                // top: '10%',
                // bottom: '10%',
                textStyle: {
                    fontSize: 10
                },
                data: []
            },
            grid: {
                x: "1%",
                x2: "30%",
                show: true
            },
            dataZoom: {
                type: "slider",
                show: true,
                showDetail: true,
                realtime: true
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
                name: 'revenue'
            },
            series: []
        }
    }


    componentDidMount() {
        var myChart = echarts.init(document.getElementById('queryChart'));
        myChart.setOption({
            title: {text: 'ECharts 入门示例'},
            tooltip: {},
            xAxis: {
                data: ["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"]
            },
            yAxis: {},
            series: [{
                name: '销量',
                type: 'bar',
                data: [5, 20, 36, 10, 10, 26]
            }]
        });
    }


    render() {
        return (
            <div className="queryfig-card">
                <Card title="Query Result Figure: ">
                    <QueryChart/>
                    <div id="queryChart" style={{width:"100%", height:500}}></div>
                </Card>
            </div>
        );
    }
}
