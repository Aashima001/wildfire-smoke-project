"""
=============================================================
API TEST 3 — NOAA / NWS (National Weather Service)
=============================================================
PURPOSE : Fetch current weather observations from CA stations
AUTH    : No API key — but MUST send User-Agent header
OUTPUT  : Prints weather data and saves to noaa_sample.json

IMPORTANT: NOAA blocks requests without a proper User-Agent.
           Replace the email below with your actual email.
=============================================================
"""

import requests
import json

# ── CONFIG ────────────────────────────────────────────────
# NOAA requires this header — replace with your actual email
HEADERS = {
    "User-Agent": "wildfire-smoke-project, aashimataneja01@gmail.com",
    "Accept"    : "application/geo+json",
}

BASE_URL = "https://api.weather.gov"

# A sample of California weather station IDs
# Full list: https://api.weather.gov/stations?state=CA
CA_STATION_SAMPLES = [
    "KSJC",   # San Jose
    "KLAX",   # Los Angeles
    "KSAC",   # Sacramento
    "KFAT",   # Fresno
    "KSAN",   # San Diego
]


# ── STEP 1: Get list of all CA weather stations ───────────
def fetch_ca_stations():
    """
    Fetch all weather stations in California.
    In the real pipeline this gives us the full station list to loop through.
    """
    print("\n📡 Step 1: Fetching California weather station list...")

    url = f"{BASE_URL}/stations"
    params = {
        "state": "CA",   # filter to California
        "limit": 10,     # only 10 for testing (there are 500+ real stations)
    }

    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=30)
        print(f"   HTTP Status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            stations = data.get("features", [])
            print(f"   ✅ Found {len(stations)} stations (showing first 10 of many)")

            for s in stations[:3]:  # show first 3
                props = s.get("properties", {})
                print(f"   Station: {props.get('stationIdentifier', 'N/A')} "
                      f"— {props.get('name', 'N/A')}")
            return stations
        else:
            print(f"   ❌ Error: {response.status_code} — {response.text[:300]}")
            return []

    except Exception as e:
        print(f"   ❌ Error fetching stations: {e}")
        return []


# ── STEP 2: Get latest observation for one station ────────
def fetch_station_observation(station_id):
    """
    Fetch the most recent weather observation for a given station ID.
    """
    url = f"{BASE_URL}/stations/{station_id}/observations/latest"
    print(f"\n📡 Step 2: Fetching latest observation for station: {station_id}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        print(f"   HTTP Status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            props = data.get("properties", {})

            # Temperature comes in Celsius from NOAA
            temp_c = props.get("temperature", {})
            temp_value = temp_c.get("value")  # can be None if missing

            # Humidity
            humidity = props.get("relativeHumidity", {})
            humidity_value = humidity.get("value")

            # Wind speed (in km/h from NOAA)
            wind_speed = props.get("windSpeed", {})
            wind_value = wind_speed.get("value")

            # Wind direction in degrees (0=N, 90=E, 180=S, 270=W)
            wind_dir = props.get("windDirection", {})
            wind_dir_value = wind_dir.get("value")

            # Precipitation (last hour, in mm)
            precip = props.get("precipitationLastHour", {})
            precip_value = precip.get("value")

            # Timestamp of observation
            timestamp = props.get("timestamp", "N/A")

            print(f"   ✅ Observation retrieved!")
            print(f"     Timestamp      : {timestamp}")
            print(f"     Temperature    : {temp_value}°C")
            print(f"     Humidity       : {humidity_value}%")
            print(f"     Wind Speed     : {wind_value} km/h")
            print(f"     Wind Direction : {wind_dir_value}°")
            print(f"     Precipitation  : {precip_value} mm")

            return {
                "station_id"      : station_id,
                "timestamp"       : timestamp,
                "temperature_c"   : temp_value,
                "humidity_pct"    : humidity_value,
                "wind_speed_kmh"  : wind_value,
                "wind_direction"  : wind_dir_value,
                "precipitation_mm": precip_value,
            }

        elif response.status_code == 404:
            print(f"   ⚠️  No recent observation for station {station_id}")
        else:
            print(f"   ❌ Error: {response.status_code}")

    except Exception as e:
        print(f"   ❌ Error: {e}")

    return None


# ── MAIN FUNCTION ─────────────────────────────────────────
def fetch_noaa_data():
    print("=" * 60)
    print("TESTING: NOAA / NWS Weather API")
    print("=" * 60)

    all_observations = []

    # Step 1: Get station list (for reference)
    fetch_ca_stations()

    # Step 2: Get observations for our sample stations
    print("\n" + "-" * 40)
    print("Fetching observations for sample stations:")
    print("-" * 40)

    for station_id in CA_STATION_SAMPLES:
        obs = fetch_station_observation(station_id)
        if obs:
            all_observations.append(obs)

    # Save results
    if all_observations:
        output_path = "noaa_sample.json"
        with open(output_path, "w") as f:
            json.dump(all_observations, f, indent=2)
        print(f"\n💾 Results saved to: {output_path}")
        print(f"   Total observations: {len(all_observations)}")
    else:
        print("\n⚠️  No observations collected.")

    print("\n" + "=" * 60)
    print("NOAA TEST COMPLETE")
    print("=" * 60)


# ── RUN ───────────────────────────────────────────────────
if __name__ == "__main__":
    fetch_noaa_data()
