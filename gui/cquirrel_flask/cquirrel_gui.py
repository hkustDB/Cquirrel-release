import os
import logging

if __name__ == '__main__':
    LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
    DATE_FORMAT = "%Y/%m/%d %H:%M:%S"
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)

    from cquirrel_app import create_app
    from cquirrel_app import socketio

    app = create_app(os.getenv('FLASK_CONFIG_NAME') or 'default')
    # app.run(debug=True)
    socketio.run(app, debug=True)
