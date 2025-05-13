import json
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from logger import LOGGER


class CantCreateResponse(Exception):
    pass


@dataclass
class Response:
    latitude: float
    longitude: float
    weather_classification: str
    weather_description: str
    country: str
    location_name: str
    temperature: float
    feels_like_temperature: float
    minimal_temperature: float
    maximal_temperature: float
    pressure: int
    humidity: int
    wind_speed: float
    wind_direction_degree: int
    wind_gust: float
    clouds: int
    dt: int

    def create_temperature_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict({
            'datetime': [
                datetime.fromtimestamp(self.dt).strftime("%Y-%m-%d %H:%M:%S")
            ],
            'temp': [self.temperature],
            'feels_like': [self.feels_like_temperature],
            'temp_min': [self.minimal_temperature],
            'temp_max': [self.maximal_temperature],
            'pressure': [self.pressure]})

    def create_wind_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict({
            'datetime': [datetime.fromtimestamp(self.dt).strftime("%Y-%m-%d %H:%M:%S")],
            'speed': [self.wind_speed],
            'deg': [self.wind_direction_degree]})


class ResponseProcessor:
    @staticmethod
    def process(raw_response: str) -> Response:
        try:
            loaded_response: dict = json.loads(raw_response)
            response: Response = Response(
                loaded_response.get('coord', {}).get('lat'),
                loaded_response.get('coord', {}).get('lon'),
                loaded_response.get('weather', {})[0].get('main'),
                loaded_response.get('weather', [{}])[0].get('description'),
                loaded_response.get('sys', {}).get('country'),
                loaded_response.get('name'),
                loaded_response.get('main', {}).get('temp'),
                loaded_response.get('main', {}).get('feels_like'),
                loaded_response.get('main', {}).get('temp_min'),
                loaded_response.get('main', {}).get('temp_max'),
                loaded_response.get('main', {}).get('pressure'),
                loaded_response.get('main', {}).get('humidity'),
                loaded_response.get('wind', {}).get('speed'),
                loaded_response.get('wind', {}).get('deg'),
                loaded_response.get('wind', {}).get('gust'),
                loaded_response.get('clouds', {}).get('all'),
                loaded_response.get('dt')
            )
        except json.JSONDecodeError as e:
            LOGGER.info('Error when handling response from request, raised with message %s', e)
            raise CantCreateResponse
        return response
