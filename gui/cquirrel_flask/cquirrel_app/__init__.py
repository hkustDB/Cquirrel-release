from confluent_kafka import Consumer
from multiprocessing import Process, Queue
import os
import time
import logging
import threading
import socket
import heapq

from flask import Flask
from flask_bootstrap import Bootstrap
from flask_socketio import SocketIO
from flask_cors import CORS
from flask import current_app

from config import BaseConfig, config_options

from cquirrel_app import cquirrel_utils

bootstrap = Bootstrap()
# socketio = SocketIO()
# socketio = SocketIO(cors_allowed_origins="*", cors_credentials=False, async_mode='eventlet')
# socketio = SocketIO(cors_allowed_origins="*", cors_credentials=False, async_mode='threading')
socketio = SocketIO(cors_allowed_origins="*", cors_credentials=False, ping_timeout=50000)

# import eventlet
# eventlet.monkey_patch()

cors = CORS(resources={r"/*": {"origins": "*"}})

stop_send_data_thread_flag = False
send_data_control = "send"
queue = Queue()


def r_run_socket_server(queue):
    cquirrel_utils.kill_5001_port()
    cquirrel_utils.kill_5001_port()
    print("r_run_socket_server: ")
    sk = socket.socket()  # 创建 socket 对象
    host = "localhost"  # 获取本地主机名
    port = 5001  # 设置端口号
    sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    sk.bind((host, port))
    sk.listen(5)
    conn, addr = sk.accept()
    t_data = ""

    while True:
        data = conn.recv(1024)
        if data:
            # print("*** socket recv data: ", data)
            t_data = t_data + str(data, "utf-8")
            while True:
                close_quotation_idx = t_data.find(')')
                if close_quotation_idx == -1:
                    break
                else:
                    line = t_data[:close_quotation_idx + 1]
                    t_data = t_data[close_quotation_idx + 1:]
                    queue.put(line)
                    # print("*** socket queue recv line: ", line)


def create_app(config_name):
    cquirrel_utils.clean_flink_output_files()

    app = Flask(__name__)
    app.secret_key = os.urandom(24)
    app.config.from_object(config_options[config_name])
    config_options[config_name].init_app(app)
    app.config['CORS_HEADERS'] = 'Content-Type'

    bootstrap.init_app(app)
    socketio.init_app(app)
    cors.init_app(app)

    from .r import r as r_blueprint
    app.register_blueprint(r_blueprint)

    return app


@socketio.on('connect')
def socketio_connect():
    print("socketio connected")


@socketio.on('disconnect')
def socketio_disconnect():
    print('socketio disconnected')


@socketio.on('r_stop_send_data_thread', namespace='/ws')
def r_stop_server_send_data_thread(data):
    global stop_send_data_thread_flag
    stop_send_data_thread_flag = True


@socketio.on('r_send_data_control', namespace='/ws')
def r_sever_send_data_control(data):
    print('received send_data_control: ', data)
    global send_data_control
    if data['command'] == "pause":
        send_data_control = "pause"
    elif data['command'] == 'restart':
        send_data_control = "send"
    else:
        print("unknown data command: ", data)


@socketio.on('r_set_top_n_value', namespace='/ws')
def r_set_top_n_value(data):
    top_n_value = data['top_n_value']


def r_send_codgen_log_and_retcode(codegen_log, retcode):
    logging.info("r_send_codgen_log_and_retcode: ")
    socketio.start_background_task(target=_send_codgen_log_and_retcode, codegen_log=codegen_log, retcode=retcode)


def r_send_information_data(information_data):
    logging.info("r_send_information_data: ")
    socketio.start_background_task(target=_send_information_data, information_data=information_data, )


def _send_information_data(information_data):
    socketio.emit('r_information_data', {"information_data": information_data}, namespace='/ws')


def _send_codgen_log_and_retcode(codegen_log, retcode):
    logging.info("_send_codgen_log_and_retcode: ")
    if retcode == 0:
        socketio.emit('r_set_step', {"step": 3}, namespace='/ws')
    else:
        socketio.emit('r_set_step', {"step": 1}, namespace='/ws')
    socketio.emit('r_codegen_log', {"codegen_log": codegen_log, "retcode": retcode}, namespace='/ws')


def r_set_step_to(n):
    print("r_set_step_to: ", n)
    socketio.start_background_task(target=_set_step_to, n=n)


def _set_step_to(n):
    print("_set_step_to: ", n)
    socketio.emit('r_set_step', {"step": n}, namespace='/ws')


def r_send_message(m_type, message):
    socketio.start_background_task(target=_send_message, m_type=m_type, message=message)


def _send_message(m_type, message):
    socketio.sleep(0.001)
    socketio.emit('r_message', {"m_type": m_type, "message": message}, namespace='/ws')


def stop_send_data_thread():
    global stop_send_data_thread_flag
    stop_send_data_thread_flag = True
    while not queue.empty():
        queue.get(block=True)

def r_send_query_result_data_from_file():
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.3
    socketio.emit('r_start_to_send_data', {"status": "start"}, namespace='/ws')

    total_data = {}
    x_timestamp = []
    max_record = {}

    global stop_send_data_thread_flag
    global send_data_control
    stop_send_data_thread_flag = False
    with open(BaseConfig.OUTPUT_DATA_FILE, 'r') as f:
        while True:
            if send_data_control == "pause":
                while True:
                    if send_data_control == "send":
                        break
                    if stop_send_data_thread_flag:
                        break
                    socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
            if stop_send_data_thread_flag:
                break
            socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)

            line = f.readline()
            if line:
                line_list = line.strip().lstrip('(').rstrip(')').split('|')
                for i in range(len(line_list)):
                    line_list[i] = line_list[i].strip()

                if len(line_list) == 3:
                    # print("r_figure_data: ", str(line_list))
                    socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
                    socketio.emit('r_figure_data', {"isTopN": 0, "data": line_list}, namespace='/ws')
                else:
                    # TopN

                    N = BaseConfig.TopNValue
                    aggregate_name = cquirrel_utils.get_aggregate_name_from_information_json()

                    line_list_len = len(line_list)
                    x_timestamp_idx = line_list_len - 1
                    attribute_length = int((line_list_len - 1) / 2)
                    aggregate_name_idx = get_aggregate_name_idx(aggregate_name, line_list)
                    aggregate_value_idx = get_aggregate_value_idx(aggregate_name_idx)

                    # get current key_tag
                    key_tag = ""
                    for i in range(attribute_length):
                        if i == aggregate_value_idx:
                            continue
                        key_tag = key_tag + line_list[attribute_length + i] + ":" + line_list[i] + ","
                    key_tag = key_tag[: (len(key_tag) - 1)]

                    # add the new value into total_data
                    if key_tag not in total_data:
                        # if total_data is not null, in each key, add the last value
                        if len(total_data) != 0:
                            # add other key_tag
                            for key in total_data:
                                tmpValue = total_data.get(key)
                                total_data[key] = [x for x in tmpValue] + [tmpValue[-1]]
                        # add the new key_tag
                        total_data[key_tag] = []
                        for i in range(len(x_timestamp)):
                            total_data[key_tag].append(0.0)
                        total_data[key_tag].append(float(line_list[aggregate_value_idx]))
                    else:
                        for key in total_data:
                            tmpValue = total_data.get(key)
                            total_data[key] = [x for x in tmpValue] + [tmpValue[-1]]
                        total_data[key_tag].pop(len(total_data[key_tag]) - 1)
                        total_data[key_tag].append(float(line_list[aggregate_value_idx]))

                    # add timestamp
                    x_timestamp.append(line_list[x_timestamp_idx])
                    for key in total_data:
                        max_record[key] = (total_data[key])[-1]

                    # get top N key_tag
                    topN = sorted(max_record.items(), key=lambda item: item[1], reverse=True)
                    topN = topN[:N]
                    top_value_data = {}
                    for k, v in topN:
                        top_value_data[k] = total_data[k]

                    logging.info("send: " + str(line_list))
                    socketio.emit('r_figure_data',
                                  {'isTopN': 1,
                                   'data': line_list,
                                   'x_timestamp': x_timestamp,
                                   "top_value_data": top_value_data}, namespace='/ws')
            else:
                r_set_step_to(5)
                break

def r_send_query_result_data_from_socket(q):
    # print("r_send_query_result_data_from_socket: ")
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.3
    socketio.emit('r_start_to_send_data', {"status": "start"}, namespace='/ws')

    total_data = {}
    x_timestamp = []
    max_record = {}

    global stop_send_data_thread_flag
    global send_data_control
    stop_send_data_thread_flag = False
    while True:
        # print("send_data_control=", send_data_control, "stop_send_data_thread_flag=", stop_send_data_thread_flag)
        if send_data_control == "pause":
            while True:
                if send_data_control == "send":
                    break
                if stop_send_data_thread_flag:
                    break
                socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
        if stop_send_data_thread_flag:
            break
        socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)

        if q.empty():
            print("r_send_query_result_data_from_socket: queue is empty")
        line = q.get()
        print("r_send_query_result_data_from_socket: line: ", line)
        if line:
            line_list = line.strip().lstrip('(').rstrip(')').split('|')
            for i in range(len(line_list)):
                line_list[i] = line_list[i].strip()

            if len(line_list) == 3:
                # print("r_figure_data: ", str(line_list))
                socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
                socketio.emit('r_figure_data', {"isTopN": 0, "data": line_list}, namespace='/ws')
            else:
                # TopN

                N = BaseConfig.TopNValue
                aggregate_name = cquirrel_utils.get_aggregate_name_from_information_json()

                line_list_len = len(line_list)
                x_timestamp_idx = line_list_len - 1
                attribute_length = int((line_list_len - 1) / 2)
                aggregate_name_idx = get_aggregate_name_idx(aggregate_name, line_list)
                aggregate_value_idx = get_aggregate_value_idx(aggregate_name_idx)

                # get current key_tag
                key_tag = ""
                for i in range(attribute_length):
                    if i == aggregate_value_idx:
                        continue
                    key_tag = key_tag + line_list[attribute_length + i] + ":" + line_list[i] + ","
                key_tag = key_tag[: (len(key_tag) - 1)]

                # add the new value into total_data
                if key_tag not in total_data:
                    # if total_data is not null, in each key, add the last value
                    if len(total_data) != 0:
                        # add other key_tag
                        for key in total_data:
                            tmpValue = total_data.get(key)
                            total_data[key] = [x for x in tmpValue] + [tmpValue[-1]]
                    # add the new key_tag
                    total_data[key_tag] = []
                    for i in range(len(x_timestamp)):
                        total_data[key_tag].append(0.0)
                    total_data[key_tag].append(float(line_list[aggregate_value_idx]))
                else:
                    for key in total_data:
                        tmpValue = total_data.get(key)
                        total_data[key] = [x for x in tmpValue] + [tmpValue[-1]]
                    total_data[key_tag].pop(len(total_data[key_tag]) - 1)
                    total_data[key_tag].append(float(line_list[aggregate_value_idx]))

                # add timestamp
                x_timestamp.append(line_list[x_timestamp_idx])
                for key in total_data:
                    max_record[key] = (total_data[key])[-1]

                # get top N key_tag
                topN = sorted(max_record.items(), key=lambda item: item[1], reverse=True)
                topN = topN[:N]
                top_value_data = {}
                for k, v in topN:
                    top_value_data[k] = total_data[k]

                logging.info("send: " + str(line_list))
                socketio.emit('r_figure_data',
                              {'isTopN': 1,
                               'data': line_list,
                               'x_timestamp': x_timestamp,
                               "top_value_data": top_value_data}, namespace='/ws')
        else:
            r_set_step_to(5)
            break


def get_aggregate_value_idx(aggregate_name_idx):
    if aggregate_name_idx < 0:
        logging.error("aggregate_name_idx is negative, aggregate_name_idx = ", aggregate_name_idx)
        return aggregate_name_idx
    return int((aggregate_name_idx - 1) / 2)


def get_aggregate_name_idx(aggregate_name, line_list):
    for i in range(len(line_list)):
        if line_list[i].lower() == aggregate_name.lower():
            return i
    logging.error("can not find aggregate_name, aggregate_name = ", aggregate_name, " line_list = ", line_list)
    return -1
