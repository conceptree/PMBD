import requests
import threading
from queue import Queue

# Função para obter a localidade baseada na latitude e longitude com cache
location_cache = {}

def get_location(lat, lon):
    coordinates = f"{lat},{lon}"
    if coordinates in location_cache:
        return location_cache[coordinates]

    api_key = 'dd3ebb4b796a4d798c05113e0ba4895b'
    url = f"https://api.opencagedata.com/geocode/v1/json?q={lat}+{lon}&key={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        results = response.json().get('results')
        if results:
            location = results[0]['formatted']
            location_cache[coordinates] = location
            return location
    return None

def worker(q, lat_lon_list, result_list):
    while True:
        index = q.get()
        if index is None:
            break
        lat, lon = lat_lon_list[index]
        location = get_location(lat, lon)
        result_list[index] = location
        q.task_done()

def geocode_locations(lat_lon_list, num_worker_threads=10):
    q = Queue()
    result_list = [None] * len(lat_lon_list)

    threads = []
    for i in range(num_worker_threads):
        t = threading.Thread(target=worker, args=(q, lat_lon_list, result_list))
        t.start()
        threads.append(t)

    for index in range(len(lat_lon_list)):
        q.put(index)

    q.join()

    for i in range(num_worker_threads):
        q.put(None)
    for t in threads:
        t.join()

    return result_list
