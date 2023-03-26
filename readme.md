# Apache Airflow DAGs Examples

Use cases for Apache Airflow.

## Apache Airflow Intallation

https://airflow.apache.org/docs/apache-airflow/stable/start.html

    #Airflow needs a home. `~/airflow` is the default, but you can put it somewhere else if you prefer (optional)
    export AIRFLOW_HOME=~/airflow

	#Set the version of Airflow you want to install
    AIRFLOW_VERSION=2.5.2
	
    #Set the version of Python you want to use / has installed
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)" 
    
	#Get the corresponding constraints file
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    # For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.5.2/constraints-3.7.txt

    #Install Airflow using the constraints file
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    
    # The Standalone command will initialise the database, make a user, and start all components for you.
    airflow standalone
    
    # Visit localhost:8080 in the browser and use the admin account details
    # shown on the terminal to login.


## Use cases:

* [Data Ingestion]() - DAG for data ingestion from same sources using BashOperator to submit Spark Jobs.
* [Star Schema processing]() - DAG for Datawarehouse data processing.
* [Smart Tables processing]() - DAG for Smart tables data processing.


## DAGs

#### CIA_SYS_BUS_ING_PROJ

![DAG for data ingestion](/docs/imgs/cia_sys_bus_ing_proj.png)

#### CIA_SYS_BUS_ENH_PROJ

![DAG for Datawarehouse data processing](/docs/imgs/cia_sys_bus_enh_proj.png)

#### CIA_SYS_BUS_EXP_PROJ

![DAG for Smart tables data processing](/docs/imgs/cia_sys_bus_exp_proj.png)

## Deployment

### Deploy DAGs

Deploy the [/DAGs/]() files from into your Airflow DAGs folder.

Usually it is located in */usr/local/airflow/dags/*

You can check/change your DAGs folder in *airflow.cfg* file, property *dags_folder*.


### Configure Variables

Add the following variables to your Airflow instance using the Airflow UI:

* **ing_path** - Path to the folder with the scripts and data to be ingested.
* **enh_path** - Path to the folder with the scripts process the DataWarehouse.
* **exp_path** - Path to the folder with the scripts process the Smart Tables.
* **engine_script** - Path to the Spark engine script.
* **base_path** - Path to the base project folder.
* **odate** - Date of the data to be processed.
