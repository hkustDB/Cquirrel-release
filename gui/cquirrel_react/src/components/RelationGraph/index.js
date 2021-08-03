import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import {Button, Card} from 'antd';

import "./relationgraph-card.css"


export default class RelationGraph extends Component {
    constructor(props) {
        super(props);
    }

    render() {
        return (
            <div className="relationgraph-card">
                <Card title="Relations Graph: ">
                    {
                        this.props.showRelationGraph ? (
                            <div style={{textAlign: "left"}}>
                                Base Relations:<br/>
                                <div>
                                    {this.props.relationsData.relations.map((rel) => {
                                        return <Button shape="round" size="small">{rel}</Button>
                                    })}
                                </div>
                                Binary Predicates:<br/>
                                <div>
                                    {this.props.relationsData.binary.map((rel) => {
                                        return <Button shape="round" size="small">{rel}</Button>
                                    })}
                                </div>
                                Unary Predicates:<br/>
                                <div>
                                    {
                                        this.props.relationsData.unary.map(
                                            (rel) => {
                                                var tmpUnary = [];
                                                for (var key in rel) {
                                                    if (rel[key] instanceof Array) {
                                                        var len = rel[key].length
                                                        for (var i = 0; i < len; i++) {
                                                            tmpUnary.push(<Button shape="round" size="small">{rel[key][i]}</Button>)
                                                        }
                                                    }
                                                }
                                                return tmpUnary
                                            })
                                    }
                                </div>

                            </div>
                        ) : null
                    }
                </Card>
            </div>
        )
    }
}
