import React, {Component} from 'react'
import MonacoEditor from 'react-monaco-editor';
import {Input, Card, Button} from 'antd'


export default class SqlEditor extends Component {
    constructor(props) {
        super(props);
        this.state = {
            code: '',
        }
    }

    editorDidMount(editor, monaco) {
        console.log('editorDidMount', editor);
        editor.focus();
    }

    onChange(newValue, e) {
        console.log('onChange', newValue, e);
    }

    render() {
        const {TextArea} = Input;
        const options = {
            selectOnLineNumbers: true,
            wordHighlighter: true,
            automaticLayout: true,
            formatOnPaste: true,
            lineNumbers: true,
            minimap: {
                enabled: false
            }
        };

        return (
            <div style={{textAlign: "left"}}>
                <MonacoEditor
                    width="100%"
                    height="400"
                    language="sql"
                    theme="vs-light"

                    value={this.state.code}
                    options={options}
                    onChange={this.onChange}
                    editorDidMount={this.editorDidMount}
                />
            </div>
        );
    }
}
