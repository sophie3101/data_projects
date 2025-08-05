import requests
import pandas as pd
from datetime import datetime
wmo_code_descriptions = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Drizzle: Light",
    53: "Drizzle: Moderate",
    55: "Drizzle: Dense",
    61: "Rain: Light",
    63: "Rain: Moderate",
    65: "Rain: Heavy",
    71: "Snow fall: Light",
    73: "Snow fall: Moderate",
    75: "Snow fall: Heavy",
    80: "Rain showers: Light",
    81: "Rain showers: Moderate",
    82: "Rain showers: Violent",
    95: "Thunderstorm: Slight or moderate",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail"
}
def get_weather_data(start_time='2024-01-01', stop_time='2024-12-31', lat=40.7128, lng=74.0060):
    try:
        current_datetime = datetime.now()
        current_datetime = current_datetime.strftime("%Y-%m-%d")
        if stop_time > current_datetime:
            stop_time = current_datetime
        url=f"https://archive-api.open-meteo.com/v1/era5?latitude={lat}&longitude={lng}&start_date={start_time}&end_date={stop_time}&hourly=temperature_2m,weather_code"
        print(url)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        times = data['hourly']['time']
        temps= data['hourly']['temperature_2m']
        weather_codes = data['hourly']['weather_code']
        weathers = [wmo_code_descriptions.get(code) for code in weather_codes]
        df = pd.DataFrame({'timestamp':times, 'temperature':temps,'weather':weathers})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # print(df.head())
        return df
    except Exception as e:
        print(e)
        raise
if __name__=="__main__":    
    # get_weather_data()
    get_weather_data(start_time='2025-01-01', stop_time='2025-12-31', lat=40.7128, lng=74.0060)