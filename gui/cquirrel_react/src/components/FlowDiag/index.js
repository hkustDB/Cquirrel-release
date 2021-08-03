import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import {Button, Card} from 'antd';
import {Graph, DataUri} from '@antv/x6'
import {DagreLayout} from '@antv/layout'

import "./flowdiag-card.css"
import {DownloadOutlined} from "@ant-design/icons";


export default class Flowdiag extends Component {
    static defaultProps = {
        diag: "Here are flow diagram.",
    }

    constructor(props) {
        super(props);
        this.containerRef = React.createRef();

        // this.state.modelData = 1;
        this.state = {
            showFlowDiag: false,
            flowDiagData: [],
            modelData: {},
        }
    }

    data = {
        // 节点
        nodes: [
            {
                id: 'node1',
                width: 120,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Source / Input Stream Splitter Reader',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'node2',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Customer Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'node3',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Orders Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'node4',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Lineitem Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'node5',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Aggregation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'node6',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Sink',
                            width: -10,
                        }
                    },
                },
            },
        ],
        // 边
        edges: [
            {
                source: 'node1',
                target: 'node2',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'node2',
                target: 'node3',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'node3',
                target: 'node4',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'node4',
                target: 'node5',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'node5',
                target: 'node6',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'REBALANCE',
                            }
                        },
                    },
                ],
            },
            {
                source: 'node1',
                target: 'node3',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'node1',
                target: 'node4',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
        ],
    }

    q1ModelData = {
        nodes: [
            {
                id: 'source',
                width: 120,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Source / Input Stream Splitter Reader',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'lineitem',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Lineitem Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'aggregate',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Aggregation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'sink',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Sink',
                            width: -10,
                        }
                    },
                },
            },
        ],
        edges: [
            {
                source: 'source',
                target: 'lineitem',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'lineitem',
                target: 'aggregate',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'aggregate',
                target: 'sink',
                labels: [ { attrs: { label: { text: 'REBALANCE',} }, }, ],
            },
        ],
    }

    q3ModelData = {
        // 节点
        nodes: [
            {
                id: 'source',
                width: 120,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Source / Input Stream Splitter Reader',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'customer',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Customer Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'orders',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Orders Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'lineitem',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Lineitem Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'aggregate',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Aggregation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'sink',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Sink',
                            width: -10,
                        }
                    },
                },
            },
        ],
        // 边
        edges: [
            {
                source: 'source',
                target: 'customer',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'customer',
                target: 'orders',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'orders',
                target: 'lineitem',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'lineitem',
                target: 'aggregate',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'aggregate',
                target: 'sink',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'REBALANCE',
                            }
                        },
                    },
                ],
            },
            {
                source: 'source',
                target: 'orders',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
            {
                source: 'source',
                target: 'lineitem',
                labels: [
                    {
                        attrs: {
                            label: {
                                text: 'HASH',
                            }
                        },
                    },
                ],
            },
        ],
    }


    q4ModelData = {
        nodes: [
            {
                id: 'source',
                width: 120,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Source / Input Stream Splitter Reader',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'orders',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Orders Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'lineitem',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Lineitem Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'aggregate',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Aggregation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'sink',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Sink',
                            width: -10,
                        }
                    },
                },
            },
        ],
        edges: [
            {
                source: 'source',
                target: 'orders',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'orders',
                target: 'lineitem',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'lineitem',
                target: 'aggregate',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'aggregate',
                target: 'sink',
                labels: [{attrs: {label: {text: 'REBALANCE',}},},],
            },
            {
                source: 'source',
                target: 'lineitem',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
        ],
    }

    q9ModelData = {
        nodes: [
            {
                id: 'source',
                width: 120,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Source / Input Stream Splitter Reader',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'nation',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Nation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'supplier',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Supplier Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'partsupps',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Partsupps Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'part',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Part Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'partsuppp',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Partsuppp Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'lineitemps',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Lineitemps Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'orders',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Orders Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'lineitemorder',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Lineitemorder Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'aggregate',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Aggregation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'sink',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Sink',
                            width: -10,
                        }
                    },
                },
            },
        ],
        edges: [
            {
                source: 'source',
                target: 'nation',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'source',
                target: 'supplier',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'source',
                target: 'partsupps',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'source',
                target: 'part',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'source',
                target: 'lineitemps',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'source',
                target: 'orders',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'nation',
                target: 'supplier',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'supplier',
                target: 'partsupps',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'part',
                target: 'partsuppp',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'partsupps',
                target: 'partsuppp',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'partsuppp',
                target: 'lineitemps',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'orders',
                target: 'lineitemorder',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'lineitemps',
                target: 'lineitemorder',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'lineitemorder',
                target: 'aggregate',
                labels: [ { attrs: { label: { text: 'HASH',} }, }, ],
            },
            {
                source: 'aggregate',
                target: 'sink',
                labels: [ { attrs: { label: { text: 'REBALANCE',} }, }, ],
            },
        ],
    }

    q10ModelData = {
        nodes: [
            {
                id: 'source',
                width: 120,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Source / Input Stream Splitter Reader',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'customer',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Customer Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'orders',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Orders Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'lineitem',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Lineitem Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'nation',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Nation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'aggregate',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Aggregation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'sink',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Sink',
                            width: -10,
                        }
                    },
                },
            },
        ],
        edges: [
            {
                source: 'source',
                target: 'nation',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'nation',
                target: 'customer',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'customer',
                target: 'orders',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'orders',
                target: 'lineitem',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'lineitem',
                target: 'aggregate',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'aggregate',
                target: 'sink',
                labels: [{attrs: {label: {text: 'REBALANCE',}},},],
            },
            {
                source: 'source',
                target: 'customer',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'source',
                target: 'orders',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'source',
                target: 'lineitem',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
        ],
    }

    q14ModelData = {
        nodes: [
            {
                id: 'source',
                width: 120,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Source / Input Stream Splitter Reader',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'part',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Part Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'lineitem',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Lineitem Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'aggregate',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Aggregation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'sink',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Sink',
                            width: -10,
                        }
                    },
                },
            },
        ],
        edges: [
            {
                source: 'source',
                target: 'part',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'part',
                target: 'lineitem',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'lineitem',
                target: 'aggregate',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'aggregate',
                target: 'sink',
                labels: [{attrs: {label: {text: 'REBALANCE',}},},],
            },
            {
                source: 'source',
                target: 'lineitem',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
        ],
    }

    q16ModelData = {
        nodes: [
            {
                id: 'source',
                width: 120,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Source / Input Stream Splitter Reader',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'part',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Part Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'supplier',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Supplier Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'partsupps',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Partsupps Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'partsuppp',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Partsuppp Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'aggregate',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Aggregation Process Function',
                            width: -10,
                        }
                    },
                },
            },
            {
                id: 'sink',
                width: 100,
                height: 60,
                attrs: {
                    body: {
                        fill: 'rgb(177,218,255)',   // 背景颜色
                        stroke: 'rgb(24,144,255)',  // 边框颜色
                    },
                    text: {
                        textWrap: {
                            text: 'Data Sink',
                            width: -10,
                        }
                    },
                },
            },
        ],
        edges: [
            {
                source: 'source',
                target: 'part',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'source',
                target: 'supplier',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'source',
                target: 'partsupps',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'supplier',
                target: 'partsupps',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'part',
                target: 'partsuppp',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'partsupps',
                target: 'partsuppp',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'partsuppp',
                target: 'aggregate',
                labels: [{attrs: {label: {text: 'HASH',}},},],
            },
            {
                source: 'aggregate',
                target: 'sink',
                labels: [{attrs: {label: {text: 'REBALANCE',}},},],
            },

        ],
    }

    download_flow_diagram = () => {
        if (this.graph) {
            this.graph.toSVG((dataUri: string) => {
                // 下载
                DataUri.downloadDataUri(DataUri.svgToDataUrl(dataUri), 'flow_figure.svg')
            })
        }
    }

    shouldComponentUpdate(nextProps, nextState) {
        var model_data = {}

        if (nextProps !== this.props) {

            if (Array.isArray(nextProps.flowDiagData)) {
                let relations = JSON.stringify(nextProps.flowDiagData.sort())

                if (relations == JSON.stringify(["lineitem"].sort())) {     //q1, q6
                    model_data = this.q1ModelData
                } else if (relations == JSON.stringify(["lineitem", "orders", "customer"].sort())) {    //q3, q18
                    model_data = this.q3ModelData
                } else if (relations == JSON.stringify(["lineitem", "orders"].sort())) {    //q4, q12
                    model_data = this.q4ModelData
                } else if (relations ==
                    JSON.stringify(["lineitemorder", "nation", "part", "supplier", "lineitemps", "partsupps", "orders", "partsuppp"]
                        .sort())) {     // q9
                    model_data = this.q9ModelData
                } else if (relations == JSON.stringify(["lineitem", "orders", "customer", "nation"].sort())) {    //q10
                    model_data = this.q10ModelData
                } else if (relations == JSON.stringify(["lineitem", "part"].sort())) {    //q14,  q19
                    model_data = this.q14ModelData
                } else if (relations == JSON.stringify(["supplier", "part", "partsupps", "partsuppp"].sort())) {    //q16
                    model_data = this.q16ModelData
                } else {

                }
            }
        }

        if (nextProps.showFlowDiag) {
            const graph = new Graph({
                container: ReactDOM.findDOMNode(this.containerRef.current),
                height: 290,
                width: "100%",
                mousewheel: {
                    enabled: true,
                    factor: 1.1,
                    minScale: 0.1,
                    maxScale: 10,
                },
                panning: true,
            })
            this.graph = graph;

            const dagreLayout = new DagreLayout({
                type: 'dagre',
                rankdir: 'LR',
                align: 'DR',
                ranksep: 45,
                nodesep: 25,
                controlPoints: true,
                nodeSize: [80, 40],
            })

            const model = dagreLayout.layout(model_data)

            graph.fromJSON(model)
            graph.centerContent()
        } else {
            if (this.graph) {
                this.graph.dispose()
            }
        }
        return true
    }

    render() {

        return (

            <div className="flowdiag-card">
                <Card title="Flow Diagram: " extra={
                    <div>
                        <Button size="small" type="link" icon={<DownloadOutlined/>}
                                onClick={this.download_flow_diagram}>Download Flow Diagram</Button>
                    </div>
                }>
                    <div className="flowdiag-content">
                        <div id="flowdiag-container" ref={this.containerRef}/>
                    </div>
                </Card>
            </div>
        )
    }
}
