from datetime import datetime as dt, timedelta
from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.operators.python import PythonOperator
from helpers.data_extraction import fetch_weather_data
from helpers.data_transformation import transform_data_into_csv
from helpers.model_training import compute_model_score
from airflow.sensors.filesystem import FileSensor
from datetime import timedelta
from helpers.data_preparation import prepare_data
from helpers.model_training import compute_model_score, select_best_model, X, y
from helpers.airflow_utils import (
    config_variables,
    create_pool_if_not_exists,
    create_sub_dag,
)
from helpers.utils import read_json
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor


# Configuration of variables
env_vars = read_json("dags/helpers/airflow_config.json")

config_variables(env_vars)
# Pool creation
create_pool_if_not_exists("ml_pool", 1, "Definition of the pool dedicated to evaluation.", 0)
# Data preparation for learning. Returns two sets, X (the features) and y (the target)
X, y = prepare_data("/app/clean_data/fulldata.csv")


# Definition of the DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": dt(2023, 12, 29),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "pool": "ml_pool",
    "retry_delay": timedelta(minutes=1),
}

# Initialization of the DAG with its ID, description, schedule, and tags.
dag = DAG(
    dag_id="dag_openweather",
    description="ML DAG with airflow",
    tags=["evaluation", "airflow"],
    schedule_interval="* * * * *",
    default_args=default_args,
    catchup=False,
)

# This task allows waiting for a certain delay before moving on to the next task.
waiter_sub_dag = SubDagOperator(
    task_id="waiter_sub_dag",
    subdag=create_sub_dag(dag_id=f"{dag.dag_id}.waiter_sub_dag", schedule_interval="@once", start_date=dt.now()),
    dag=dag,
)

#This task saves weather data in JSON format.
task_1 = PythonOperator(
    task_id="save_json_data",
    python_callable=fetch_weather_data,
    dag=dag,
)
# This task transforms the last 20 JSON files into CSV format saved in data.csv.
task_2 = PythonOperator(
    task_id="get_20_last_json_files_to_csv",
    python_callable=transform_data_into_csv,
    op_kwargs={"n_files": 20, "filename": "data.csv"},
    dag=dag,
)
# This task transforms all JSON files into CSV format saved in fulldata.csv.
task_3 = PythonOperator(
    task_id="get_all_json_files_to_csv",
    python_callable=transform_data_into_csv,
    op_kwargs={"filename": "fulldata.csv"},
    dag=dag,
)
# This task computes and evaluates a linear regression model.
task_4_1 = PythonOperator(
    task_id="compute_linear_regression",
    python_callable=compute_model_score,
    op_kwargs={"model": LinearRegression(), "X": X, "y": y, "xcom_key": "score_lr"},
    dag=dag,
)

# This task computes and evaluates a decision tree model
task_4_2 = PythonOperator(
    task_id="compute_decision_tree",
    python_callable=compute_model_score,
    op_kwargs={
        "model": DecisionTreeRegressor(),
        "X": X,
        "y": y,
        "xcom_key": "score_dt",
    },
    dag=dag,
)
# This task computes and evaluates a random forest model
task_4_3 = PythonOperator(
    task_id="compute_random_forest",
    python_callable=compute_model_score,
    op_kwargs={
        "model": RandomForestRegressor(),
        "X": X,
        "y": y,
        "xcom_key": "score_rf",
    },
    dag=dag,
)
# This task selects the best model among those evaluated previously.
task_5 = PythonOperator(
    task_id="choose_best_model", python_callable=select_best_model, dag=dag
)


# This sensor ensures that the data.csv file is available in the clean_data folder
clean_data_sensor = FileSensor(
    task_id="check_clean_data",
    fs_conn_id="clean_data_fs",
    filepath="/app/clean_data/data.csv",
    poke_interval=20,
    dag=dag,
    timeout=5 * 20,
    mode="reschedule",
)
# This sensor ensures that the fulldata.csv file is available in the clean_data folder
clean_fulldata_sensor = FileSensor(
    task_id="check_clean_fulldata",
    fs_conn_id="clean_fulldata_fs",
    filepath="/app/clean_data/fulldata.csv",
    poke_interval=20,
    dag=dag,
    timeout=5 * 20,
    mode="reschedule",
)

# Definition of the order and dependency of tasks.
task_1 >> waiter_sub_dag  >> [task_2, task_3] 
task_2>>clean_data_sensor
task_3 >> clean_fulldata_sensor>>[task_4_1, task_4_2, task_4_3]  >> task_5
