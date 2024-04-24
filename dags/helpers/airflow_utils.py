from airflow.models import Variable
from airflow.models import Pool
from airflow.utils.db import provide_session
import json
import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.models import Connection
from airflow.settings import Session
from datetime import timedelta
from helpers.utils import read_json

def create_sub_dag(dag_id, schedule_interval, start_date):
    sub_dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        default_args={"start_date": start_date},
    )
    # This task checks if the delay has already been executed.
    waiter_condition = PythonOperator(
    task_id='waiter_condition',
    python_callable=should_run_waiter,
    dag=sub_dag,
)
    # This sensor checks that there are at least 20 JSON files available in the raw_files folder.
    check_minimum_files_sensor = ShortCircuitOperator(
    task_id="check_min_20_json_files",
    python_callable=check_mininum_files,
    op_kwargs={"threshold": 20, "data_folder": "/app/raw_files/"},
    dag=sub_dag,
)
    
    waiter = TimeDeltaSensor(
    task_id="waiter",
    delta=timedelta(minutes=15),  # Wait for 15 minutes
    dag=sub_dag
)
    waiter_condition >> waiter >>check_minimum_files_sensor
    return sub_dag

def create_file_connection(conn_id, file_path):
    """
    Create or update an Airflow connection of type 'File'. The file path is stored in the 'host' field.

    Args:
        conn_id (str): The connection id for the connection.
        file_path (str): The file path that the connection will point to.
    """

    # Start a session
    session = Session()

    # Check if the connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if existing_conn:
        # Update the existing connection
        existing_conn.host = file_path  # Store the file path in the host field
        session.commit()
        print(f"Updated the connection: {conn_id}")
    else:
        # Create a new connection
        new_conn = Connection(
            conn_id=conn_id,
            conn_type='File',  # Specifying the type as 'File'
            host=file_path  # Using the 'host' field to store the file path
        )
        session.add(new_conn)
        session.commit()
        print(f"Created new connection: {conn_id}")

    # Close session
    session.close()

def check_mininum_files(threshold=20, data_folder="/app/raw_files/"):
    """This function checks if the number of JSON files in the specified folder (raw_files) reaches a given minimum threshold. 
    If the number of JSON files is less than the threshold, a ValueError is raised."""
        # Count the number of JSON files in the specified folder
    json_files_count = len(
            [file for file in os.listdir(data_folder) if file.endswith(".json")]
        )
        # Check if the number of files reaches the threshold
    if json_files_count < threshold:
            raise ValueError(f"Less than {threshold} JSON files found: {json_files_count}")
    return True

def should_run_waiter():
    """This function checks if the 'waiter' has already executed"""
    run_status_key = 'WAITER_HAS_RUN'
    
    has_run = Variable.get(run_status_key, default_var="0")
    
    if has_run=="0":
        Variable.set(run_status_key, "1")
        return True
    # If it has run, return False to prevent further runs
    return False

def set_airflow_variable_if_not_exists(key, value):
    """
    Creating Airflow variables in case of absence
    """
    # Check if the variable already exists
    try:
        # Try to get the variable's value
        var_value = Variable.get(key, default_var=None)
        if var_value is None:
            # The variable doesn't exist, so set it
            Variable.set(key=key, value=value)
            print(f"Variable '{key}' set to '{value}'.")
        else:
            # The variable exists, no action needed
            print(f"Variable '{key}','{value} already exists. No action taken.")
    except KeyError:
        # In case of a KeyError, set the variable
        Variable.set(key, value)
        print(f"Variable '{key}' set to '{value}'.")

@provide_session
def create_pool_if_not_exists(
    pool_name, slots, description="", include_deferred=0, session=None
):
    """Create a pool if it does not exist"""
    # # Check if the pool exists
    existing_pool = session.query(Pool).filter(Pool.pool == pool_name).first()

    if existing_pool is None:
        # Create the pool if it does not exist
        new_pool = Pool(
            pool=pool_name,
            slots=slots,
            description=description,
            include_deferred=include_deferred,
        )
        session.add(new_pool)
        session.commit()
        print(f"Pool '{pool_name}' created with {slots} slots.")
    else:
        # Le pol existe déjà
        print(f"Pool '{pool_name}' already exists. No action taken.")

def config_variables(env_vars):
    print("Running setup")
    set_airflow_variable_if_not_exists(
        "AIRFLOW_OPENWEATHER_API_URL", env_vars.get("AIRFLOW_OPENWEATHER_API_URL")
    )
    set_airflow_variable_if_not_exists(
        "AIRFLOW_OPENWEATHER_API_KEY", env_vars.get("AIRFLOW_OPENWEATHER_API_KEY")
    )
    set_airflow_variable_if_not_exists("CITIES", json.dumps(env_vars.get("CITIES")))
    set_airflow_variable_if_not_exists("WAITER_HAS_RUN", env_vars.get("WAITER_HAS_RUN"))

    create_file_connection('clean_data_fs', '/app/clean_data/data.csv')
    create_file_connection('clean_fulldata_fs', '/app/clean_data/fulldata.csv')


    True
    print("Setup completed")

if __name__ == "__main__":
    env_vars = read_json("dags/helpers/airflow_config.json")
    print(json.dumps(env_vars, indent=3))
    print(json.dumps(env_vars.get("CITIES")))
    config_variables(env_vars)
    create_pool_if_not_exists(
        "ml_pool", 1, "ML pool setup", 0
    )
