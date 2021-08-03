import cquirrel_app
from . import r
from .. import cquirrel_utils
from config import BaseConfig
from flask import render_template, request, send_from_directory, flash, redirect, url_for
from werkzeug.utils import secure_filename

import os
import shutil
import logging
import json
import time
import threading
from multiprocessing import Process, Queue
from cquirrel_app import socketio


@socketio.on('r_connect', namespace='/ws')
def r_socket_connect(data):
    print("r_socket_connect: ", data)
    socketio.emit('r_socket_connect', {'data': 1})


@r.route('/r')
def index():
    return "here are flask r."
    # aju_utils.clean_codegen_log_and_generated_jar();
    # return render_template('index.html')


@r.route('/r/upload', methods=['POST'])
def upload_json_file():
    # start socket server background process
    from multiprocessing import Process
    p = Process(target=cquirrel_app.r_run_socket_server, args=(cquirrel_app.queue,))
    p.start()

    cquirrel_app.r_set_step_to(0)
    cquirrel_app.stop_send_data_thread()

    f = request.files['json_file']
    uploaded_json_filename = secure_filename(f.filename)
    query_idx = cquirrel_utils.get_query_idx(uploaded_json_filename)
    uploaded_json_file_save_path = os.path.join(BaseConfig.JSON_FILE_UPLOAD_PATH, uploaded_json_filename)

    # check if the upload dir exists or not
    if not os.path.exists(BaseConfig.JSON_FILE_UPLOAD_PATH):
        os.makedirs(BaseConfig.JSON_FILE_UPLOAD_PATH)

    # save the json file to server
    f.save(uploaded_json_file_save_path)

    # check if the uploaded file is json file
    if not cquirrel_utils.is_json_file(uploaded_json_file_save_path):
        # remove the uploaded non json file
        if os.path.exists(uploaded_json_file_save_path):
            os.remove(uploaded_json_file_save_path)

    # remove the older generated-code directory
    if os.path.isdir(BaseConfig.GENERATED_CODE_DIR):
        shutil.rmtree(BaseConfig.GENERATED_CODE_DIR)
        logging.info('remove the generated-code directory.')

    cquirrel_app.r_set_step_to(1)
    # call the codegen to generate a jar file
    codegen_log_result, retcode = cquirrel_utils.run_codegen_to_generate_jar(uploaded_json_file_save_path, query_idx)

    cquirrel_app.r_send_codgen_log_and_retcode(codegen_log_result, retcode)
    print('retcode: ', retcode)
    if retcode != 0:
        cquirrel_app.r_send_message("error", "codegen failed!")
        # cquirrel_app.r_set_step_to(1)
        return "codegen failed."

    print("query id: ", str(query_idx))
    cquirrel_utils.r_run_flink_task(BaseConfig.GENERATED_JAR_FILE, cquirrel_app.queue)

    return codegen_log_result


@r.route("/r/download_codegen_log")
def r_download_codegen_log():
    print("r_download_codegen_log")
    if os.path.exists(BaseConfig.CODEGEN_LOG_FILE):
        return send_from_directory(BaseConfig.CODEGEN_LOG_PATH, 'codegen.log', as_attachment=True)
    else:
        pass


@r.route("/r/download_generated_jar")
def r_download_generated_jar():
    print("r_download_generated_jar")
    if os.path.exists(BaseConfig.GENERATED_JAR_FILE):
        return send_from_directory(BaseConfig.GENERATED_JAR_PATH, 'generated.jar', as_attachment=True)
    else:
        pass


@r.route("/r/save_settings", methods=['POST'])
def r_save_settings():
    save_settings_data = request.data

    settings = json.loads(str(save_settings_data, "utf-8"))
    BaseConfig.REMOTE_FLINK = settings.get("remote_flink")
    BaseConfig.REMOTE_FLINK_URL = settings.get("remote_flink_url")
    BaseConfig.FLINK_HOME_PATH = settings.get("flink_home_path")
    BaseConfig.FLINK_PARALLELISM = settings.get("flink_parallelism")

    return settings


@r.route("/r/submit_sql", methods=['POST'])
def r_submit_sql():
    cquirrel_app.stop_send_data_thread()

    data_str = str(request.data, 'utf-8')
    sql_content = json.loads(data_str)['sql']
    print(sql_content)

    # information_data = cquirrel_utils.r_run_codegen_to_generate_json(sql_content, BaseConfig.GENERATED_JSON_PATH)
    # cquirrel_app.r_send_information_data(information_data)

    from multiprocessing import Process
    p = Process(target=cquirrel_app.r_run_socket_server, args=(cquirrel_app.queue,))
    p.start()

    # remove the older generated-code directory
    if os.path.isdir(BaseConfig.GENERATED_CODE_DIR):
        shutil.rmtree(BaseConfig.GENERATED_CODE_DIR)
        logging.info('remove the generated-code directory.')

    # input_data_pattern = cquirrel_utils.r_get_input_data_pattern(information_data)
    # call the codegen to generate a jar file
    information_data, codegen_log_result, retcode = cquirrel_utils.r_run_codegen_to_generate_jar2(sql_content)
    cquirrel_app.r_send_information_data(information_data)
    cquirrel_app.r_send_codgen_log_and_retcode(codegen_log_result, retcode)
    print('retcode: ', retcode)
    if retcode != 0:
        cquirrel_app.r_send_message("error", "codegen failed!")
        return "codegen failed."

    cquirrel_utils.r_run_flink_task(BaseConfig.GENERATED_JAR_FILE, cquirrel_app.queue)

    return codegen_log_result
