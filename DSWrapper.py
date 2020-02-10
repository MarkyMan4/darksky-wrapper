'''

Wrapper for the Dark Sky API. Formats responses as a DataFrame or dictionary.

All times are given in UTC. It is the duty of the user of this class to convert time zones.

'''

import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta

class DSWrapper:

    api_key = ''

    # Set the API key upon creation of object.
    def __init__(self, api_key):
        self.api_key = api_key

    # convert all unix time stamps in Dark Sky response to date times
    def convert_unix_time(self, weather_data):
        weather_data['currently']['time'] = datetime.fromtimestamp(weather_data['currently']['time']).strftime('%Y-%m-%d %H:%M')

        # try statements are because some responses may not contain all these keys
        # if it doesn't contain that key, pass and continue execution
        try:
            for d in weather_data['minutely']['data']:
                d['time'] = datetime.fromtimestamp(d['time']).strftime('%Y-%m-%d %H:%M')
        except:
            pass

        try:
            for d in weather_data['hourly']['data']:
                d['time'] = datetime.fromtimestamp(d['time']).strftime('%Y-%m-%d %H:%M')
        except:
            pass

        try:
            for d in weather_data['daily']['data']:
                d['time'] = datetime.fromtimestamp(d['time']).strftime('%Y-%m-%d %H:%M')
        except:
            pass

        try:
            for a in weather_data['alerts']:
                a['time'] = datetime.fromtimestamp(a['time']).strftime('%Y-%m-%d %H:%M')
                a['expires'] = datetime.fromtimestamp(a['expires']).strftime('%Y-%m-%d %H:%M')
        except:
            pass
        
    # Make a Dark Sky request with the specified parameters.
    # Returns the JSON as a dictionary.
    def make_request(self, lat, lon, time=None):
        url = f'https://api.darksky.net/forecast/{self.api_key}/{lat},{lon}'

        if time:
            url = url + ',' + time

        r = requests.get(url)
        weather = r.json()

        self.convert_unix_time(weather)
        
        return weather

    # Makes an API call without specifying a time. The response contains current weather,
    # minutely weather for the current hour, hourly data for the next 48 hours, daily data
    # for the next 7 days and any current weather alerts.
    #
    # The 'raw' argument specifies how the data should be returned.
    # If raw is True, it will return the raw JSON as a dictionary.
    # If raw is False or not specified, it will return the data as a dictionary with the following keys/values:
    # 'alerts'   : <dictionary of alerts>
    # 'current'  : <dictionary of current weather conditions>
    # 'minutely' : <DataFrame of minute-by-minute data for the next hour>
    # 'hourly'   : <DataFrame of hourly data for the next 48 hours>
    # 'daily'    : <DataFrame of weather data for the next seven days>
    def get_current_data(self, lat, lon, raw=False):
        data = {}
        cur_weather = self.make_request(lat, lon)

        if not raw:
            data.update({'alerts': cur_weather['alerts']})                            # dictionary
            data.update({'current': cur_weather['currently']})                        # dictionary
            data.update({'minutely': pd.DataFrame(cur_weather['minutely']['data'])})  # DataFrame
            data.update({'hourly': pd.DataFrame(cur_weather['hourly']['data'])})      # DataFrame
            data.update({'daily': pd.DataFrame(cur_weather['daily']['data'])})        # DataFrame
        else:
            data = cur_weather

        return data

    # Get hourly weather data for a 24 hour span.
    # Time must be given in the format YYYY-mm-ddTHH:MM:SS
    # If time is not provided, it will default to hour 00:00 of today
    def get_hourly_weather(self, lat, lon, time=None):
        hourly = {}

        # default to today
        if not time:
            time = datetime.now().strftime('%Y-%m-%d')
            time += 'T00:00:00'

        hourly = self.make_request(lat, lon, time)
        hourly = hourly['hourly']['data']

        return pd.DataFrame(data=hourly)

    # Returns a DataFrame containing hourly data for the range of dates specified
    # for the location specified.
    #
    # Dark Sky API doesn't have an option to get a range of dates, so this function is
    # very useful. However, it makes one API call for each day in the range of dates.
    # So if a free tier Dark Sky account is being used, the range of dates should not be 
    # more than 1,000 days (assuming no previous API calls have been made that day)
    def get_hourly_range(self, lat, lon, start_time, end_time):
        time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
        end = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S')
        weather_data = pd.DataFrame()
        
        while time < end:
            time_str = time.strftime('%Y-%m-%dT%H:%M:%S')
            response = self.make_request(lat, lon, time_str)
            daily_data = pd.DataFrame(data=response['hourly']['data'])

            if weather_data.empty:
                weather_data = daily_data
            else:
                weather_data = weather_data.append(daily_data)

            time += timedelta(days=1)
        
        weather_data = weather_data.reset_index(drop=True)

        return weather_data
