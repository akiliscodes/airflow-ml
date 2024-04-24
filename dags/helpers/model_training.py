import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump
from helpers.data_preparation import prepare_data
from airflow.models import TaskInstance

X, y = prepare_data("/app/clean_data/fulldata.csv")
def compute_model_score(model, X, y, task_instance=None, xcom_key=None):
    # computing cross val
    cross_validation = cross_val_score(
        model, X, y, cv=3, scoring="neg_mean_squared_error"
    )

    model_score = cross_validation.mean()
    task_instance.xcom_push(key=xcom_key, value=model_score)


def train_and_save_model(model, X, y, path_to_model="./app/model.pckl"):
    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), "saved at ", path_to_model)
    dump(model, path_to_model)


def select_best_model(**context):
    """
    This function selects the best model. 
    It first retrieves the xcom from the previous training tasks, extracts the best score, and then retrains on the best one.
    """
    task_instance: TaskInstance = context["ti"]
    scores = [
        task_instance.xcom_pull(task_ids="compute_linear_regression", key="score_lr"),
        task_instance.xcom_pull(task_ids="compute_decision_tree", key="score_dt"),
        task_instance.xcom_pull(task_ids="compute_random_forest", key="score_rf"),
    ]

    models = [
        {"model": LinearRegression()},
        {"model": DecisionTreeRegressor()},
        {"model": RandomForestRegressor()},
    ]
    scores = [float(score) if score is not None else None for score in scores]

    for model, score in zip(models, scores):
        model["score"] = score
    models_scores = models.copy()
    max_model = max(models_scores, key=lambda x: x["score"])
    train_and_save_model(
        max_model.get("model"), X, y, "/app/clean_data/best_model.pickle"
    )
    return max_model.get('score')


if __name__ == "__main__":
    X, y = prepare_data("./clean_data/fulldata.csv")

    models = [
        {"model": LinearRegression()},
        {"model": DecisionTreeRegressor()},
        {"model": RandomForestRegressor()},
    ]

    for idx, model in enumerate(models):
        score = compute_model_score(model.get("model"), X, y)
        models[idx]["score"] = score
    max_model = max(models, key=lambda x: x["score"])
    train_and_save_model(max_model.get("model"), X, y, "/app/clean_data/best_model.pickle")
    print(max_model)
