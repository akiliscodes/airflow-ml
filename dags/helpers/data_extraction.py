import requests
from dotenv import load_dotenv
import os
from airflow.models import Variable
from helpers.utils import save_data_as_json
load_dotenv()


def get_weather_data(city):
    """This function is a wrapper used to query the OpenWeather API.

    Args:
        city (string): City for which weather information is desired

    Returns:
        dict: Weather information for the city of interest
    """

    api_url = Variable.get(
        "AIRFLOW_OPENWEATHER_API_URL", os.getenv("AIRFLOW_OPENWEATHER_API_URL")
    )
    api_key = Variable.get(
        "AIRFLOW_OPENWEATHER_API_KEY", os.getenv("AIRFLOW_OPENWEATHER_API_KEY")
    )

    if not api_key:
        raise ValueError("No API key found. Please check your .env file.")

    url = f"{api_url}q={city}&appid={api_key}"

    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 401:
        raise ValueError("Invalid API key. Please check your API key and try again.")
    else:
        error_message = response.json().get("message", "An error occurred")
        raise Exception(
            f"Error fetching weather data: {error_message} (Status code: {response.status_code})"
        )

def get_weather_for_cities(cities):
    """Wrapper permettant de récupérer les inforamtions météo pour une liste de villes

    Args:
        cities (List): Liste de villes d'intérêt dont on souhaite récupérer les informations météo

    Returns:
        dict: Dictionnaire agrégeant les informations météo pour une liste de ville d'intérêt
    """
    all_cities_weather = {}
    for city in cities:
        try:
            weather_data = get_weather_data(city)
            all_cities_weather[city] = weather_data
        except Exception as e:
            print(f"Error fetching data for {city}: {e}")

    return all_cities_weather

def fetch_weather_data():
    """Cette fonction récupère les données météorologiques pour une liste de villes et
    les enregistre sous forme de fichier JSON.
    """
    cities = Variable.get("CITIES", deserialize_json=True, default_var=["paris", "london", "washington"])
    weather_data = get_weather_for_cities(cities=cities)
    save_data_as_json(weather_data)


if __name__ == "__main__":
    cities = ["paris", "london", "washington"]
    fetch_weather_data(cities=cities)
