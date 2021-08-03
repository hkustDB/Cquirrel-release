# Cquirrel
This is the release of the Cquirrel Demonstration.
<br>
The dev repos are:<br>
Cquirrel-frontend: <https://github.com/hkustDB/Cquirrel-Frontend> <br>
Cquirrel-core (will release soon): <https://github.com/hkustDB/Cquirrel-core-release> 


## Environment Requirement
### Operating System
We run the demo in MacOS Big Sur 11.5. For now, we do not support Windows.

### Software Dependencies
Here are the software version in which we run the cquirrel demo.
* In MacOS Big Sur 11.5 x86_64
Google Chrome 91.0.4472.106 (Stable)  
Python 3.8.5  
Java 1.8.0_261  
Scala 2.12.13
Maven 3.6.3   
sbt 1.3.13  
yarn 1.22.10  
Flink 1.11.2

## Directory Description
* `DemoTools/DataGenerator` : This is the tool to generate our input data files. We can edit config file to meet our requirements for the input data.
* `codegen` : This is the codegen components, which can transform a sql to a flink program.
* `gui/cquirrel_flask` : This is the web backend of the demo, which controlles the whole procedures in demo.
* `gui/cquirrel_react` : This is the web frontend of the demo, which presents the views in demo.


## Running Steps

### Download relevant materials
1. Download and install the following relevant software according to the dependencies in the above.
  - Google Chrome
  - Python  -> 3.8.5
  - Java    -> 1.8.0_261 
  - Scala   -> 2.12.13
  - Maven   -> 3.6.3 
  - sbt     -> 1.3.13
  - yarn    -> 1.22.10
  - Flink   -> 1.11.2     <https://archive.apache.org/dist/flink/flink-1.11.2/flink-1.11.2-bin-scala_2.12.tgz>


### Boot the Apache Flink
1. Download the Apache Flink and unzip the package into your computer. The download link is provided above.

2. Change the directory into `flink-1.11.2`, and start the flink cluster.  
`> cd flink-1.11.2`  
`> bin/start-cluster.sh`


### Git clone the repository
1. Clone the repository to your own computer.  
`> git clone <repository url in the github>`  
`> cd  Cquirrel-Frontend`

### Generate the input data
1. Download the TPC-H Tools. The webpage link of the TPC-H Tools is <http://tpc.org/tpc_documents_current_versions/current_specifications5.asp>. You should find the `Download TPC-H_Tools_v3.0.0.zip` link and click to get the package.

2. You can put the `tpc-h_tools` pacakge into a directory and unzip it. Then you will get a directory which contains `dbgen`, `dev-tools`, and `specification.pdf`, etc. 

3. Copy the `makefile.suite` file in the `dbgen` directory to `makefile`, for the purpose of suiting your own operating system and platforms. 
`> cd dbgen`
`> cp makefile.suite makefile`

4. Edit the `makefile` to suit your own environment. However, we should make sure that the data in the generated `*.tbl` file should be seperated by `"|"`. Perhaps you might need to revise the source code of the `tpc-h_tools`. Take my example:
```
################
## CHANGE NAME OF ANSI COMPILER HERE
################
CC      = GCC
# Current values for DATABASE are: INFORMIX, DB2, TDAT (Teradata)
#                                  SQLSERVER, SYBASE, ORACLE, VECTORWISE
# Current values for MACHINE are:  ATT, DOS, HP, IBM, ICL, MVS, 
#                                  SGI, SUN, U2200, VMS, LINUX, WIN32 
# Current values for WORKLOAD are:  TPCH
DATABASE= ORACLE 
MACHINE = LINUX
WORKLOAD = TPCH
```

5. In the `dbgen` directory, run `make` to compile the source code, and we can get an executable file which also called `dbgen`.  
`> make makefile`

6. In the `dbgen` directory, run the `dbgen` executable file to generate `.tbl` data files. The `-s` means the size of generated data. The `-s 1` means that it will generate 1GB data.   
`> ./dbgen -s 1 -vf`

7. Copy all `.tbl` data file into the `DemoTools/DataGenerator` directory.  
`> cp *.tbl <Path_of_DemoTools/DataGenerator>`

8. In the `DemoTools/DataGenerator` directory, generate the input data. The default config file is `config_all.ini`. We can get all inserted without deleted tpch data, which is named `input_data_all.csv`.   
`> cd DemoTools/DataGenerator`  
`> python DataGenerator.py`

### Install the Cquirrel core into your maven repository
1. Make sure the Apache Maven with appropriate version is installed in your computer.

2. Install `cquirrel.jar` into your mvn repository.  
`> mvn install:install-file -Dfile=./cquirrel.jar -DgroupId=org.hkust -DartifactId=AJU -Dversion=1.0-SNAPSHOT -Dpackaging=jar`

### Boot the web backend of the demo
1. Make sure that you have already installed the Python 3.8.5 and the relevant `pip` tools, which can install python dependencies.

2. Install the python dependencies. Make sure that the `pip` should belong to the Python3 rather than the Python2.  
`> cd Cquirrel-Frontend/gui/cquirrel_flask/`  
`> pip install -r requirements.txt`

3. Edit the config file of the web backend in the `cquirrel_flask` directory. Make sure the paths of `INPUT_DATA_FILE` and `OUTPUT_DATA_FILE` is correct.
The `INPUT_DATA_FILE` path should be the same with the `input_data_all.csv` in 8th step of generating the input data. 

4. Boot the web backend of the demo in the `cquirrel_flask` directory.  
`> python cquirrel_gui.py`

### Boot the web frontend of the demo
1. Make sure that you have already installed the `npm` and `yarn` in your computer.

2. Change the directory into `cquirrel_react`.  
`> cd Cquirrel-Frontend/gui/cquirrel_react/`  

3. Build the web frontend of the demo.  
`> yarn build` 

4. Boot the web frontend of the demo.
`> yarn start`


### Open the web page of the demo
1. Input the address `http://localhost:3000` in your Google Chrome. And press `Enter`.

2. Click the tpc-h template sql, and click the "Submit SQL" button. Wait and there will be some results in your webpage.

3. Click the top-left `Cquirrel` in the navigate bar, will reset all the contents and then your can submit another sql.
