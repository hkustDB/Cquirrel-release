# Cquirrel-GUI

This is the gui part of the Cquirrel project. 

For now, the project only supports Chrome.


# Environments
macOS BigSur 11.2.2

Python 3.8.2

Flink 1.11.2

Scala 2.12

Flask 1.1.2

React 17.0.1

In this project, we use python3 by default.

# Installation

## Install Flask
First, we go to the backend folder `cquirrel_flask` to install Flask and other relevant python3 packages:
```
$ cd gui/cquirrel_flask
$ pip install -r requirements.txt
```

## Install React 
Then, we go to the frontend folder `cquirrel_react` to install React, Antd and other relevant javascript packages:
```
$ cd gui/cquirrel_react
$ yarn install
```

## Install Flink
We should download Apache Flink (version 1.11.2) to our environment. And we should set the `FLINK_HOME_PATH` value as our flink home path in `gui/cquirrel_flask/config.py`. For example, we can set as the following `FLINK_HOME_PATH='/Users/xxx/Programs/flink-1.11.2'`.

# Resources
For TPC-H Q3, we should put the input data file to `gui/cquirrel_flask/cquirrel_app/resources` folder, and rename the input data file as `input_data_q3_0.1_sliding_window.csv`.

# Config
For the backend flask project, we can edit the `config.py` to self adapt our environment.
Some important settings are `FLINK_HOME_PATH`, and `INPUT_DATA_FILE`. Please edit this before launch the GUI.

# Boot

## boot frontend 
First, we go to the frontend folder `cquirrel_react` to boot frontend:
```
$ cd gui/cquirrel_react
$ yarn start
```

## boot backend
Then, we go to the backend folder `cquirrel_flask` to boot the backend:
```
$ cd gui/cquirrel_flask
$ python3 cquirrel_gui.py
```

