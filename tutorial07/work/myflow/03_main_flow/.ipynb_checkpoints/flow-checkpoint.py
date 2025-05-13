import requests
import pandas as pd
from datetime import datetime
import pytz
from prefect import flow, task # Prefect flow and task decorators

@task
def get_weather_data(location_context={'location':None, 'province':None, 'lat':None, 'lon':None}):
    # API endpoint and parameters
    WEATHER_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"
    API_KEY = "1de9eaf371dc114af656a95cd26febd8"  # Replace with your actual API key
    location = location_context['location']
    province = location_context['province']
    
    params = {
        "lat": location_context['lat'],
        "lon": location_context['lon'],
        "appid": API_KEY,
        "units": "metric"
    }
    try:
        # Make API request
        response = requests.get(WEATHER_ENDPOINT, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        
        # Convert timestamp to datetime
        dt = datetime.now()
        thai_tz = pytz.timezone('Asia/Bangkok')
        created_at = dt.replace(tzinfo=thai_tz)

        timestamp = datetime.now()
        
        # Create dictionary with required fields
        weather_dict = {
            'timestamp': timestamp,
            'year': timestamp.year,
            'month': timestamp.month,
            'day': timestamp.day,
            'hour': timestamp.hour,
            'minute': timestamp.minute,
            'created_at': created_at,
            'province': province,  # เลือก
            'location_name': location,  # ชื่อสถาน
            'api_location': data['name'],  # ชื่อสถานจาก API
            'weather_main': data['weather'][0]['main'],
            'weather_description': data['weather'][0]['description'],
            'main.temp': data['main']['temp'],
            # Adding humidity
            'main.humidity': data['main']['humidity'],
            # Adding wind speed
            'wind.speed': data['wind']['speed'],
            # Adding precipitation (rain or snow)
            'precipitation': get_precipitation(data)
        }
        
        return weather_dict
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except KeyError as e:
        print(f"Error processing data: Missing key {e}")
        return None

# Helper function to extract precipitation data
def get_precipitation(data):
    # Check for rain data (last 1 hour)
    if 'rain' in data and '1h' in data['rain']:
        return data['rain']['1h']
    # Check for snow data (last 1 hour)
    elif 'snow' in data and '1h' in data['snow']:
        return data['snow']['1h']
    # No precipitation
    else:
        return 0.0
@flow(name="main-flow", log_prints=True)
def main_flow(parameters={}):
    locations = {
        
    "Satitram Alumni": {
        "province": "Bangkok",
        "lat": 13.752916,
        "lon": 100.618616
    },
    # เล่มสถานใหม่
    "Ramkhamhaeng University": {
        "province": "Bangkok",
        "lat": 13.7552,
        "lon": 100.6201
    },
    "Rajamangala National Stadium": {
        "province": "Bangkok",
        "lat": 13.7627,
        "lon": 100.6200
    },
    "Ramkhamhaeng Hospital": {
        "province": "Bangkok",
        "lat": 13.7485,
        "lon": 100.6265
    },
    "Hua Mak Police Station": {
        "province": "Bangkok",
        "lat": 13.7500,
        "lon": 100.6200
    },
    "Bang Kapi District Office": {
        "province": "Bangkok",
        "lat": 13.7640,
        "lon": 100.6440
    },
    "The Mall Bangkapi": {
        "province": "Bangkok",
        "lat": 13.7650,
        "lon": 100.6430
    },
    "Tawanna Market": {
        "province": "Bangkok",
        "lat": 13.7655,
        "lon": 100.6425
    },
    "Big C Huamark": {
        "province": "Bangkok",
        "lat": 13.7445,
        "lon": 100.6205
    },
    "Makro Ladprao": {
        "province": "Bangkok",
        "lat": 13.7940,
        "lon": 100.6110
    },
    "Siam Paragon": {
        "province": "Bangkok",
        "lat": 13.7458,
        "lon": 100.5343
    },
    "ICONSIAM": {
        "province": "Bangkok",
        "lat": 13.7292,
        "lon": 100.5103
    },
    "HomePro Rama 9": {
        "province": "Bangkok",
        "lat": 13.7430,
        "lon": 100.6155
    },
    "The Mall Ramkhamhaeng": {
        "province": "Bangkok",
        "lat": 13.7526,
        "lon": 100.6095
    },
    "Healthy Park": {
        "province": "Bangkok",
        "lat": 13.7565,
        "lon": 100.6275
    }
}
    
    df=pd.DataFrame([get_weather_data(
        {
            'location': location,
            'province': locations[location]['province'],
            'lat': locations[location]['lat'],
            'lon': locations[location]['lon'],
        }
    ) for location in list(locations.keys())])
    
    # lakeFS credentials from your docker-compose.yml
    ACCESS_KEY = "access_key"
    SECRET_KEY = "secret_key"
    
    # lakeFS endpoint (running locally)
    lakefs_endpoint = "http://lakefs-dev:8000/"
    
    # lakeFS repository, branch, and file path
    repo = "weather"
    branch = "main"
    path = "weather.parquet"
    
    # Construct the full lakeFS S3-compatible path
    lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"
    
    # Configure storage_options for lakeFS (S3-compatible)
    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {
            "endpoint_url": lakefs_endpoint
        }
    }
    df.to_parquet(
        lakefs_s3_path,
        storage_options=storage_options,
        partition_cols=['year','month','day','hour'],
    )