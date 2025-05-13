import os

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from logger import LOGGER


class WeatherAPICaller:
    __retry_attempts: int = 10  # number of retries if exception until calling API
    __wait_time: int = 10  # time in seconds

    def __init__(self, latitude, longitude):
        LOGGER.info(f'Get next latitude={latitude} and longitude={longitude}.')
        self.__latitude = latitude
        self.__longitude = longitude
        self.__api_key = os.environ['API_KEY']
        self.__url = (f'https://api.openweathermap.org/data/2.5/weather?'
                      f'lat={self.__latitude}&lon={self.__longitude}&appid={self.__api_key}&units=metric')

    @retry(stop=stop_after_attempt(__retry_attempts),
           wait=wait_fixed(__wait_time),
           reraise=True)
    def make_api_call(self) -> str:
        try:
            response = requests.get(self.__url).content
            LOGGER.info(f'Get raw response {response}.')
        except requests.exceptions.HTTPError as http_exception:
            LOGGER.info('HTTP Exception when making request to API, failed with message: %s', http_exception)
            raise
        except requests.exceptions.ConnectionError as connection_exception:
            LOGGER.info('Exception in connection process failed with message: %s', connection_exception)
            raise
        except requests.exceptions.Timeout as timeout_exception:
            LOGGER.info('Timeout Exception when connecting to API, failed with message: %s', timeout_exception)
            raise
        except requests.exceptions.RequestException as request_exception:
            LOGGER.info('Exception when making request to API, failed with message: %s', request_exception)
            raise
        return response
