import React, {useState} from 'react';
import {Upload, Button, Modal} from 'antd';
import {UploadOutlined} from '@ant-design/icons';

const JsonFileUploader = () => {
    const [fileList, updateFileList] = useState([]);
    const props = {
        name: 'json_file',
        action: 'http://localhost:5000/r/upload',
        fileList,
        showUploadList: true,
        accept: 'application/json',
        headers: {
            'Access-Control-Allow-Origin': '*',
        },
        beforeUpload: file => {
            return Promise.all([
                checkJsonFileNull(file),
                checkJsonFileEmpty(file),
                checkJsonFileContent(file),
            ]);
        },
        onChange: info => {
            if (info.file.status !== 'uploading') {
            }
            if (info.file.status === 'done') {
                // Modal.success({
                //     title: `The ${info.file.name} file uploaded successfully`
                // });
            } else if (info.file.status === 'error') {
                console.log(info);
                // Modal.error({
                //     title: `The ${info.file.name} file upload failed.`,
                //     content: `${info.file.error.stack}`
                // });
            }
            // file.status is empty when beforeUpload return false
            updateFileList(info.fileList.filter(file => (file.status !== 'done')));
        },
        progress: {
            strokeColor: {
                '0%': '#108ee9',
                '100%': '#87d068',
            },
            strokeWidth: 3,
            format: percent => `${parseFloat(percent.toFixed(2))}%`,
        },
    };
    return (
        <Upload {...props}>
            <Button icon={<UploadOutlined/>}>Click to Upload Json File</Button>
        </Upload>
    );
};

function checkJsonFileNull(file) {
    return new Promise(function (resolve, reject) {
        if (!file) {
            Modal.error({
                title: 'The uploaded json file is null.',
            })
            reject()
        } else {
            resolve()
        }
    })
}

function checkJsonFileEmpty(file) {
    return new Promise(function (resolve, reject) {
        if (file.size === 0) {
            Modal.error({
                title: 'The uploaded json file is empty.',
            })
            reject()
        } else {
            resolve()
        }
    })
}

function checkJsonFileContent(file) {
    return new Promise(function (resolve, reject) {
        let reader = new FileReader()
        reader.onload = e => {
            let tmpContent = e.target.result
            try {
                var tmpObj = JSON.parse(tmpContent.toString());
                if (typeof tmpObj == 'object') {
                    resolve()
                } else {
                    Modal.error({
                        title: 'The content of uploaded json file can not be transformed to a json object.',
                    })
                    reject()
                }
            } catch (e) {
                Modal.error({
                    title: 'The content of uploaded json file is not a correct json format.',
                })
                reject()
            }
        }
        reader.readAsText(file)
    })
}



export default JsonFileUploader;
