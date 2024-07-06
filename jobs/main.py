import os
import random
import uuid
import gpxpy.gpx
import simplejson as json
from confluent_kafka import SerializingProducer
import gpxpy
import gpxpy.gpx
import json
import requests
import time
from datetime import datetime, timedelta
from geopy.distance import geodesic
from dotenv import load_dotenv

load_dotenv()

# KAFKA_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://0.0.0.0:9092 
# KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092

LANDON_COORDINATES = {"latitude" : 51.5074, "longitude" : -0.1278}
BIRMINGHAM_COORDINATES = {"latitude" : 52.4862, "longitude" : -1.8904}

LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LANDON_COORDINATES['latitude']) /100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LANDON_COORDINATES['longitude']) /100

KAFKA_BOOSTRAP_SERVERS = os.getenv('KAFKA_BOOSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC','emergency_data')
FAILURES_TOPIC = os.getenv('FAILURES_TOPIC', 'failures_data')

random.seed(42)
start_time = datetime.now()
start_location = LANDON_COORDINATES.copy()

def get_extension_value(extensions, tag_name, namespace):
    ns_tag = f"{{{namespace}}}{tag_name}"
    for extension in extensions:
        if extension.tag == ns_tag:
            return extension.text
    return None

MYTRACKS_NAMESPACE = "http://mytracks.stichling.info/myTracksGPX/1/0"

def read_gpx_file(file_path):
    realtime_points = []
    with open(file_path, 'r') as gpx_file:
        gpx = gpxpy.parse(gpx_file)
        for track in gpx.tracks:
            for segment in track.segments:
                for point in segment.points:
                    speed = get_extension_value(point.extensions, 'speed', MYTRACKS_NAMESPACE)
                    length = get_extension_value(point.extensions, 'length', MYTRACKS_NAMESPACE)

                    if speed is not None:
                        speed = float(speed)
                    if length is not None:
                        length = float(length)

                    point_info = {
                        'latitude': point.latitude,
                        'longitude': point.longitude,
                        'elevation': point.elevation,
                        'time': point.time.isoformat() if point.time else None,
                        'speed': speed,
                        'length': length
                    }
                    realtime_points.append(point_info)
    return realtime_points

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
    return start_time

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY', '')
OPENWEATHER_API_URL = os.getenv('OPENWEATHER_API_URL', '')
OPENAIRQUALITY_API_URL = os.getenv('OPENAIRQUALITY_API_URL', '')

last_weather_fetch_time = None
last_weather_data = None
last_air_quality_data = None

def generate_weather_and_air_quality_data(device_id, timestamp, latitude, longitude, api_key):
    global last_weather_fetch_time, last_weather_data, last_air_quality_data
    current_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")

    if last_weather_fetch_time is None or (current_time - last_weather_fetch_time) >= timedelta(minutes=10):
        last_weather_fetch_time = current_time

        weather_url = f"{OPENWEATHER_API_URL}?lat={latitude}&lon={longitude}&appid={api_key}&units=metric"
        weather_response = requests.get(weather_url)
        if weather_response.status_code == 200:
            weather_data = weather_response.json()

            temperature = weather_data["main"]["temp"]
            weather_condition = weather_data["weather"][0]["main"]
            wind_speed = weather_data["wind"]["speed"]
            humidity = weather_data["main"]["humidity"]

            last_weather_data = {
                'temperature': temperature,
                'weatherCondition': weather_condition,
                'windSpeed': wind_speed,
                'humidity': humidity,
            }
        else:
            print("Error obtaining weather data:", weather_response.status_code)

        air_quality_url = f"{OPENAIRQUALITY_API_URL}?lat={latitude}&lon={longitude}&appid={api_key}"
        air_quality_response = requests.get(air_quality_url)
        if (air_quality_response.status_code == 200) and air_quality_response.json().get("list"):
            air_quality_data = air_quality_response.json()

            air_quality_index = air_quality_data["list"][0]["main"]["aqi"]

            last_air_quality_data = {
                'airQualityIndex': air_quality_index,
            }
        else:
            print("Error obtaining air quality data:", air_quality_response.status_code)

        if last_weather_data and last_air_quality_data:
            combined_data = {**last_weather_data, **last_air_quality_data, 'id': device_id, 'timestamp': timestamp}
            return combined_data
        else:
            return {}
    return {**last_weather_data, **last_air_quality_data,
            'id': device_id, 'timestamp': timestamp} if last_weather_data and last_air_quality_data else {}

SUSPENSION_REASONS = [
    'Failure to stop',
    'Yield sign ignored',
    'Amber light not respected',
    'Pedestrian crossing not respected',
    'Wrong way taken'
]

CENTRAL_POINT = (36.70828195217619, -4.465170324163806)
RADIUS_KM = 1.0

def is_within_radius(point, center=CENTRAL_POINT, radius=RADIUS_KM):
    return geodesic(point, center).km <= radius

last_suspension_time = None

def generate_test_drive_failure_data(device_id, timestamp, location):
    global last_suspension_time
    current_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")

    failure_data = {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'incidentId': uuid.uuid4(),
        'type': 'Test Drive Suspension',
        'timestamp': timestamp,
        'location': location,
        'description': None,
        'num_failures': 0,
    }

    if is_within_radius((location['latitude'], location['longitude'])) and \
            (last_suspension_time is None or (current_time - last_suspension_time) >= timedelta(minutes=5)):
        last_suspension_time = current_time
        failure_reason = random.choice(SUSPENSION_REASONS)
        num_failures = random.randint(1, 10)

        failure_data['description'] = f'{num_failures} suspensions: {failure_reason}'
        failure_data['num_failures'] = num_failures

    return failure_data

gpx_points = read_gpx_file('data/2022-12-02 12_12_41.gpx')

def simulate_vehicle_movement():
    global start_location

    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time



def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'latitude': location['latitude'], 
        'longitude': location['longitude'],
        'vehicleType': vehicle_type   
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident','Fire','Medical','Police','None']), 
        'timestamp': timestamp,
        'location': location,
        'status':random.choice(['Active','Resolved']),
        'description': 'Description of the incident'
    }

def generate_weather_data(device_id, timestamp, location):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temoerature': random.uniform(-5,26),
        'weatherCondition': random.choice(['Sunny','Cloudy','Rain','Snow']),
        'precipitation': random.uniform(0,25),
        'windSpeed': random.uniform(0,100),
        'humidity': random.randint(0,100),
        'airQualityIndex': random.uniform(0,500),
    }


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()

    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': {'latitude': location['latitude'], 'longitude': location['longitude']},
        'speed': random.uniform(10, 40),
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not Json serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()


def simulate_journey(producer, device_id, realtime_points, api_key):
    global last_weather_fetch_time
    # while True:
    for point in realtime_points:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id,vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'],vehicle_data['location'],'Nikon-Cam123')
        weather_data = generate_weather_and_air_quality_data(device_id, point['time'], point['latitude'],
                                                             point['longitude'], api_key)
        # weather_data = generate_weather_data(device_id,vehicle_data['timestamp'],vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id,vehicle_data['timestamp'],vehicle_data['location'])

        
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        current_time_index = realtime_points.index(point)
        if current_time_index < len(realtime_points) - 1:
            next_point_time = datetime.strptime(realtime_points[current_time_index + 1]['time'],
                                                "%Y-%m-%dT%H:%M:%S.%f%z")
            current_point_time = datetime.strptime(point['time'], "%Y-%m-%dT%H:%M:%S.%f%z")
            time_to_next_point = (next_point_time - current_point_time).total_seconds()

            time.sleep(min(time_to_next_point, 2))

    print('Simulation completed. All the GPX points have been processed.')

        # if(vehicle_data['location'][0]>= BIRMINGHAM_COORDINATES['latitude'] and  vehicle_data['location'][1]<= BIRMINGHAM_COORDINATES['longitude']):
        #     print('Vehicle has reached Birmingham. Simulation ending...')
        #     break

        # time.sleep(5)

        # time.sleep(3)



    #     weather_data = generate_weather_and_air_quality_data(device_id, point['time'], point['latitude'],
    #                                                          point['longitude'], api_key)

    #     failures_data = generate_test_drive_failure_data(device_id, point['time'],
    #                                                      {'latitude': point['latitude'],
    #                                                       'longitude': point['longitude']})
    #     if failures_data is not None:
    #         produce_data_to_kafka(producer, FAILURES_TOPIC, failures_data)

    #     produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
    #     produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)

    #     current_time_index = realtime_points.index(point)
    #     if current_time_index < len(realtime_points) - 1:
    #         next_point_time = datetime.strptime(realtime_points[current_time_index + 1]['time'],
    #                                             "%Y-%m-%dT%H:%M:%S.%f%z")
    #         current_point_time = datetime.strptime(point['time'], "%Y-%m-%dT%H:%M:%S.%f%z")
    #         time_to_next_point = (next_point_time - current_point_time).total_seconds()

    #         time.sleep(min(time_to_next_point, 2))

    # print('Simulation completed. All the GPX points have been processed.')

if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    gpx_file_path = 'data/2022-12-02 12_12_41.gpx'
    gpx_points = read_gpx_file(gpx_file_path)

    try:
        simulate_journey(producer, 'Vehicle-Rvm-001', gpx_points, OPENWEATHER_API_KEY)
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected error occurred: {e}')
