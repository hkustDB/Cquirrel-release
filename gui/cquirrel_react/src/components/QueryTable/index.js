import React, {Component} from 'react';
import { Card, Table } from 'antd';
import "./querytable-card.css"

export default class QueryTable extends Component {

    static defaultProps={
        table_cols: [],
        table_data: [],
        table_title: "Query Result Table: ",
    }

    render() {
        return (
            <div className="querytable-card" >
                <Card title={this.props.table_title} >
                        <Table
                            className="querytable-content"
                            columns={this.props.table_cols}
                            // columns={tmp_columns}
                            dataSource={this.props.table_data}
                            // dataSource={tmp_data}
                            size="small"
                            bordered
                            scroll = {{ x: "100%"}}
                        />

                </Card>

            </div>
        );
    }
}
