"""
=============================================================
API TEST 2 — EPA AirNow (Air Quality Index)
=============================================================
PURPOSE : Fetch current AQI readings for California counties
AUTH    : Requires free API key from https://docs.airnowapi.org
OUTPUT  : Prints AQI data and saves to airnow_sample.json

HOW TO USE:
  1. Replace YOUR_AIRNOW_API_KEY_HERE with your actual key
  2. Run: python test_airnow.py
=============================================================
"""

import requests
import json

# ══════════════════════════════════════════════════════════
# ⚠️  REPLACE THIS WITH YOUR ACTUAL AIRNOW API KEY
AIRNOW_API_KEY = "BB2130F9-0AC6-4981-8FAD-81857B21040B"
# ══════════════════════════════════════════════════════════

# ── CONFIG ────────────────────────────────────────────────
BASE_URL = "https://www.airnowapi.org/aq/observation/latLong/current/"

# We'll test with a few major California county centroids (lat, lon, name)
# In the real pipeline, we'll loop through all 58 counties
CA_COUNTY_SAMPLES = [
    (37.3382, -121.8863, "Santa Clara County (San Jose)"),
    (34.0522, -118.2437, "Los Angeles County"),
    (38.5816, -121.4944, "Sacramento County"),
    (36.7378, -119.7871, "Fresno County"),
]

# ── FUNCTION: Fetch AQI for one location ──────────────────
def fetch_aqi_for_location(lat, lon, location_name):
    """
    Calls the AirNow API for a specific lat/lon coordinate.
    Returns the JSON response or None if failed.
    """
    params = {
        "format"  : "application/json",  # we want JSON back
        "latitude" : lat,                 # latitude of the location
        "longitude": lon,                 # longitude of the location
        "distance" : 25,                  # look within 25 miles radius for stations
        "API_KEY"  : AIRNOW_API_KEY,      # your API key
    }

    print(f"\n📡 Fetching AQI for: {location_name} ({lat}, {lon})")

    try:
        response = requests.get(BASE_URL, params=params, timeout=60)
        print(f"   HTTP Status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()  # returns a LIST of observations

            if not data:
                # API returned empty list — no station nearby or no recent data
                print(f"   ⚠️  No data returned for this location.")
                return None

            print(f"   ✅ {len(data)} observation(s) returned")

            # Each item in the list is one pollutant reading
            for obs in data:
                print(f"   --- Observation ---")
                print(f"     Date/Hour     : {obs.get('DateObserved', 'N/A')} "
                      f"{obs.get('HourObserved', 'N/A')}:00 "
                      f"{obs.get('LocalTimeZone', '')}")
                print(f"     Pollutant     : {obs.get('ParameterName', 'N/A')}")
                print(f"     AQI Value     : {obs.get('AQI', 'N/A')}")
                print(f"     AQI Category  : {obs.get('Category', {}).get('Name', 'N/A')}")
                print(f"     Reporting Area: {obs.get('ReportingArea', 'N/A')}")
                print(f"     State         : {obs.get('StateCode', 'N/A')}")

            return data

        elif response.status_code == 400:
            print(f"   ❌ Bad request — check your API key or parameters")
            print(f"   Response: {response.text[:300]}")
        elif response.status_code == 401:
            print(f"   ❌ Unauthorized — your API key is invalid or not activated yet")
        else:
            print(f"   ❌ Error {response.status_code}: {response.text[:300]}")

    except requests.exceptions.Timeout:
        print(f"   ❌ Timeout for {location_name}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    return None


# ── MAIN FUNCTION ─────────────────────────────────────────
def fetch_airnow_data():
    print("=" * 60)
    print("TESTING: EPA AirNow API")
    print("=" * 60)

    # Check if user has replaced the placeholder key
    if AIRNOW_API_KEY == "YOUR_AIRNOW_API_KEY_HERE":
        print("\n❌ STOP: You need to add your AirNow API key!")
        print("   Open this file and replace YOUR_AIRNOW_API_KEY_HERE")
        print("   with your actual key from https://docs.airnowapi.org")
        return

    all_results = []

    # Loop through each sample county and fetch its AQI
    for lat, lon, name in CA_COUNTY_SAMPLES:
        result = fetch_aqi_for_location(lat, lon, name)
        if result:
            # Add location name to each record for reference
            for obs in result:
                obs["county_name_sample"] = name
            all_results.extend(result)

    # Save all results to a JSON file
    if all_results:
        output_path = "airnow_sample.json"
        with open(output_path, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\n💾 All results saved to: {output_path}")
        print(f"   Total observations collected: {len(all_results)}")
    else:
        print("\n⚠️  No data was collected. Check API key and try again.")

    print("\n" + "=" * 60)
    print("AIRNOW TEST COMPLETE")
    print("=" * 60)


# ── BONUS: All 58 California County Centroids ─────────────
# (Use this in the real pipeline to loop through all counties)
ALL_58_CA_COUNTIES = [
    (37.8534, -122.2477, "Alameda"),
    (40.7765, -122.5347, "Shasta"),
    (34.0522, -118.2437, "Los Angeles"),
    (37.7749, -122.4194, "San Francisco"),
    (32.7157, -117.1611, "San Diego"),
    (38.5816, -121.4944, "Sacramento"),
    (37.6879, -120.9999, "Stanislaus"),
    (36.7378, -119.7871, "Fresno"),
    (35.3733, -119.0187, "Kern"),
    (37.9577, -121.2908, "San Joaquin"),
    # ... add remaining counties for full production pipeline
    # Full list available at: https://en.wikipedia.org/wiki/List_of_counties_in_California
]

# ── RUN ───────────────────────────────────────────────────
if __name__ == "__main__":
    fetch_airnow_data()
