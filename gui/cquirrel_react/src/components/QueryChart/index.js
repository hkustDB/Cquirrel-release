import React, {Component} from "react";
import echarts from "echarts/lib/echarts"
import 'echarts/lib/chart/bar';
import 'echarts/lib/component/tooltip';
import 'echarts/lib/component/title';
import 'echarts/lib/component/legend';
import 'echarts/lib/component/grid';
import 'echarts/lib/component/dataZoom';
import 'echarts/lib/chart/line';
// import ReactEcharts from "echarts-for-react";


export default class QueryChart extends Component {

    // constructor(props) {
    //     super(props);
    //     this.state = {
    //         my_option: {
    //             animation: false,
    //             title: {
    //                 text: 'AJU Result Chart'
    //             },
    //             tooltip: {},
    //             legend: {
    //                 type: 'scroll',
    //                 orient: 'vertical',
    //                 left: '80%',
    //                 right: '0%',
    //                 // top: '10%',
    //                 // bottom: '10%',
    //                 textStyle: {
    //                     fontSize: 10
    //                 },
    //                 data: []
    //             },
    //             grid: {
    //                 x: "1%",
    //                 x2: "30%",
    //                 show: true
    //             },
    //             dataZoom: {
    //                 type: "slider",
    //                 show: true,
    //                 showDetail: true,
    //                 realtime: true
    //             },
    //             xAxis: {
    //                 type: 'category',
    //                 // min: 'dataMin',
    //                 // max: 'dataMax',
    //                 name: 'timestamp',
    //                 data: []
    //             },
    //             yAxis: {
    //                 type: 'value',
    //                 axisLabel: {
    //                     inside: true
    //                 },
    //                 // min: 'dataMin' - 1 ,
    //                 // max: 'dataMax' + 1,
    //                 name: 'revenue'
    //             },
    //             series: []
    //         },
    //     }
    // }

    constructor(props) {
        super(props);
        this.state = {
            my_option: {
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
            }
        }
    }


    componentDidMount() {
        var myChart = echarts.init(document.getElementById('queryChart'));

        // var my_option = {
        //     animation: false,
        //     title: {
        //         text: 'AJU Result Chart'
        //     },
        //     tooltip: {},
        //     legend: {
        //         type: 'scroll',
        //         orient: 'vertical',
        //         left: '80%',
        //         right: '0%',
        //         // top: '10%',
        //         // bottom: '10%',
        //         textStyle: {
        //             fontSize: 10
        //         },
        //         data: []
        //     },
        //     grid: {
        //         x: "1%",
        //         x2: "30%",
        //         show: true
        //     },
        //     dataZoom: {
        //         type: "slider",
        //         show: true,
        //         showDetail: true,
        //         realtime: true
        //     },
        //     xAxis: {
        //         type: 'category',
        //         // min: 'dataMin',
        //         // max: 'dataMax',
        //         name: 'timestamp',
        //         data: []
        //     },
        //     yAxis: {
        //         type: 'value',
        //         axisLabel: {
        //             inside: true
        //         },
        //         // min: 'dataMin' - 1 ,
        //         // max: 'dataMax' + 1,
        //         name: 'revenue'
        //     },
        //     series: []
        // };

        this.timer = setInterval(() => {
            var tmp_option = this.state.my_option;
            tmp_option.series[0].data[0] = this.state.my_option.series[0].data[0] + 1;
            myChart.setOption(tmp_option);
            // this.setState({my_option: tmp_option})
        }, 1000)

        // 绘制图表
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


    componentWillUnmount() {
        this.timer && clearInterval(this.timer);
    }

    render() {
        return (
            <div id="queryChart" style={{width: "100%", height: 500}}>
            </div>
        )
    }
}




