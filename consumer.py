import requests

lat = 30.2895
lon = -97.7368
year = 2024

try:
    url = "http://localhost:8067/FilterByHourRange?start=7&end=9"
    params = {"lat": lat, "lon": lon, "year": year}
    response = requests.get(url, params=params)

    print(f"status Code: {response.status_code}")
    print("Headers:", response.headers)

    data = response.json()
    print("Response Type:", type(data))
    print("Response Data:", data)

except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
except ValueError as e:
    print(f"Failed to parse JSON: {e}")
