import React, {Component} from 'react';
import {Steps} from 'antd';

export default class InstructSteps extends Component {
    static defaultProps={
        cur_step: 0
    }

    render() {
        const { Step } = Steps;
        return ( ""
                // <Steps current={this.props.cur_step}>
                //     <Step title="Input SQL or JSON." description="Input the SQL or JSON to start." />
                //     <Step title="Code Generating" description="The codegen is generating target jar." />
                //     <Step title="Code Generated" description="Target jar has been generated." />
                //     <Step title="Running Flink" description="Start the Apache Flink." />
                //     <Step title="Processing Data" description="Processing the data." />
                //     <Step title="Finished" description="The whole procedure has finished." />
                // </Steps>
        )
    }
}
