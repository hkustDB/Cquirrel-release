import React, {Component} from 'react'
import {Modal} from 'antd'

function AboutContent() {
    return (<div>This is the demo of AJU system, the idea of which is proposed in the paper
        <a href="https://www.cse.ust.hk/~yike/sigmod20.pdf"> "Maintaining Acyclic Foreign-Key Joins under Updates"</a>.
        <br/>
        <br/>
        If you have any problems, please contact with <a href="mailto:qwangbp@cse.ust.hk">qwangbp@cse.ust.hk</a> or <a href="mailto:czhangci@connect.ust.hk">czhangci@connect.ust.hk</a>.
    </div>)
}

export default class About extends Component {


    handleClick = (e) => {
        Modal.info({
            title: 'About',
            content: <AboutContent/>
        })
    }

    render() {
        return (
            <div onClick={this.handleClick.bind(this)}>About</div>
        )
    }
}
