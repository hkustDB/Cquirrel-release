import React, {Component} from 'react';
import {Button, Card, Input} from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import './codegen-card.css'

export default class CodegenResult extends Component {

    render() {
        const { TextArea } = Input;
        return (
            <div className="codegen-card">
                <Card title="Codegen Log: " extra={
                    <div>
                        <Button size="small" type="link" icon={<DownloadOutlined />} href="http://localhost:5000/r/download_codegen_log"> Download Codegen Log</Button>
                        <Button size="small" type="link" icon={<DownloadOutlined />} href="http://localhost:5000/r/download_generated_jar"> Download Generated Jar</Button>
                    </div>}
                >
                    <TextArea placeholder="Here are codegen logs." readOnly value={this.props.logContent}  style={{fontFamily:"monospace"}} rows={12} wrap="off"/>
                </Card>
            </div>
        )
    }
}
