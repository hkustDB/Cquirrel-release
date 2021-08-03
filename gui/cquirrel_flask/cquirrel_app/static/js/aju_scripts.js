function isEmptyObject(obj) {
    for (var n in obj) {
        return false
    }
    return true;
}

function Queue() {

    var items = [];

    this.getItemAt = function (idx) {
        return items[idx];
    }

    this.enqueue = function (element) {
        items.push(element);
    }

    this.dequeue = function () {
        return items.shift();
    }

    this.front = function () {
        return items[0];
    }

    this.isEmpty = function () {
        return items.length === 0;
    }

    this.size = function () {
        return items.length;
    }

    this.print = function () {
        return items.toString();
    }
}

function getAggregateNameIdx(aggName, line_list) {
    var target = aggName.toLowerCase();
    for (var i = 0; i < line_list.length; i++) {
        // console.log("target: "+target +", line_list[" + i + "]: " +line_list[i].trim())
        if (target == line_list[i].trim().toLowerCase()) {
            return i;
        }
    }
    return -1;
}


var myChart = echarts.init(document.getElementById('result_chart'));

var option = {
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
        // min: 'dataMin',
        // max: 'dataMax',
        name: 'timestamp',
        data: []
    },
    yAxis: {
        type: 'value',
        axisLabel: {
            inside: true
        },
        // min: 'dataMin' - 1 ,
        // max: 'dataMax' + 1,
        name: 'revenue'
    },
    series: []
};
// option.series.data.push([11,22]);
// myChart.setOption(option);

$(document).ready(function () {
    // let socket = io.connect(location.protocol + '//' + document.domain + ':' + location.port);
    const socket = io(location.protocol + '//' + document.domain + ':' + location.port);

    var local_data = [];
    var x_timestamp = [];
    var legend_data = [];
    var selected_data = {};
    var selected_queue = new Queue();
    var q6_serie = {name: "revenue", type: "line", data: []};


    socket.on('connect', () => {
        console.log('socketio connected.')
    });

    socket.on('codegen_log', (res) => {
        console.log('socketio codegen_log:', res)
    });

    socket.on('result_figure_data', function (res) {
        $('#start_to_run_flink').text('Receiving flink results data...');
        let aggregate_name = $("#aggregate_name_input").val();
        option.yAxis.name = aggregate_name;

        let query_num = res.queryNum;
        var line_list = res.data;
        for (var i = 0; i < line_list.length; i++) {
            line_list[i] = line_list[i].trim();
        }

        // q6
        if (query_num === 6) {
            option.title.text = "AJU Result Chart - TPC-H Query 6";
            q6_serie.data.push(line_list[0]);
            option.xAxis.data.push(line_list[2]);
            option.series[0] = q6_serie;
            let q6_total_length = option.xAxis.data.length;
            option.dataZoom.startValue = ((q6_total_length - 100) > 0) ? q6_total_length - 100 : 1;
        }
        // q3
        if (query_num === 3) {
            option.title.text = "AJU Result Chart - TPC-H Query 3  -- TopN";

            // let x_timestamp_idx = line_list.length - 1;
            // let aggregate_name_idx = getAggregateNameIdx(aggregate_name, line_list);
            // if (aggregate_name_idx === -1) {
            //     $("#aggregate_name_result").text("aggregate name is not correct.");
            // } else {
            //     $("#aggregate_name_result").text("");
            // }
            // let y_value_idx = (aggregate_name_idx - 1) / 2;
            // let attribute_length = (line_list.length - 1) / 2;
            // var key_tag = "";
            // for (var i = 0; i < attribute_length; i++) {
            //     if (i === y_value_idx) {
            //         continue;
            //     }
            //     key_tag = key_tag + (line_list[attribute_length + i] + ":" + line_list[i] + ", ")
            // }
            // key_tag = key_tag.substring(0, key_tag.length - 2);
            // // console.log("aggregate_name: " + aggregate_name + ", aggregate_name_idx: " + aggregate_name_idx + ", y_value_idx: " + y_value_idx + ", attribute_length: " + attribute_length +", key_tag: " + key_tag);
            // if (local_data[key_tag] === undefined) {
            //     if (local_data.length !== 0) {
            //         for (var i in local_data) {
            //             local_data[i].push(local_data[i][local_data[i].length - 1]);
            //         }
            //     }
            //
            //     local_data[key_tag] = [];
            //     for (var i = 0; i < x_timestamp.length; i++) {
            //         local_data[key_tag].push(null);
            //     }
            //     local_data[key_tag].push(line_list[y_value_idx]);
            //     legend_data.push(key_tag);
            //
            //     if (selected_queue.size() > 6) {
            //         selected_data[selected_queue.front()] = false;
            //         selected_queue.dequeue();
            //     }
            //     selected_queue.enqueue(key_tag);
            //     for (var i = 0; i < selected_queue.size(); i++) {
            //         selected_data[selected_queue.getItemAt(i)] = true;
            //     }
            //
            // } else {
            //     for (var i in local_data) {
            //         local_data[i].push(local_data[i][local_data[i].length - 1]);
            //     }
            //     local_data[key_tag].pop();
            //     local_data[key_tag].push(line_list[y_value_idx]);
            // }
            // x_timestamp.push(line_list[x_timestamp_idx]);

            x_timestamp = res.x_timestamp;
            let top_value_data = res.top_value_data;

            legend_data = []
            var series_data = []

            for (var key_tag in top_value_data) {
                legend_data.push(key_tag);
                let aserie = {name: key_tag, type: "line", data: top_value_data[key_tag]};
                series_data.push(aserie);
            }
            // option.series.push(aserie);
            option.series = series_data;
            option.legend.data = legend_data;
            option.xAxis.data = x_timestamp;
            option.dataZoom.startValue = ((x_timestamp.length - 100) > 0) ? x_timestamp.length - 100 : 1;
        }
        myChart.setOption(option);
    });

    socket.on('start_to_run_flink_job', function (res) {
        console.log('start_to_run_flink_job' + res.data.toString())
        if (res.data === 1) {
            alert('start to run flink job... please wait ~');
            $('#start_to_run_flink').text('Start to run flink job... please wait ~')

            option.xAxis.data = [];
            option.series = [];
            option.legend.data.selected = {};
            local_data = [];
            x_timestamp = [];
            legend_data = [];
            selected_data = {};
            selected_queue.clear();
            myChart.setOption(option);
        }
    });


});



