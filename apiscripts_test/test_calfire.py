"""
=============================================================
API TEST 1 — CAL FIRE / NIFC (ArcGIS REST API)
=============================================================
PURPOSE : Fetch wildfire incident data for California
AUTH    : No API key needed — public endpoint
OUTPUT  : Prints fire records and saves to calfire_sample.json
=============================================================
"""

import requests   # for making HTTP requests (calling APIs)
import json       # for reading/writing JSON data
import os         # for file/folder operations

# ── CONFIG ────────────────────────────────────────────────
# This is the ArcGIS REST API endpoint for CAL FIRE historical fire perimeters
BASE_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services"
    "/CALFIRE_Publics_FRAP_Fires_gdb/FeatureServer/0/query"
)

# These are the query parameters we send to the API
# Think of them as filters — we only want California fires, max 5 records for testing
PARAMS = {
    "where": "1=1",
    "outFields": "*",
    "resultRecordCount": 5,
    "f": "json",
    "returnGeometry": True,
}

# ── MAKE THE API CALL ──────────────────────────────────────
def fetch_calfire_data():
    print("=" * 60)
    print("TESTING: CAL FIRE / NIFC ArcGIS API")
    print("=" * 60)
    print(f"\n📡 Calling URL: {BASE_URL}")
    print(f"📋 Parameters: {json.dumps(PARAMS, indent=2)}\n")

    try:
        # requests.get() sends an HTTP GET request to the URL
        # params= automatically appends ?where=...&outFields=... to the URL
        response = requests.get(BASE_URL, params=PARAMS, timeout=30)

        # response.status_code tells us if the call succeeded
        # 200 = success, 404 = not found, 500 = server error
        print(f"✅ HTTP Status Code: {response.status_code}")

        if response.status_code == 200:
            # response.json() converts the raw text response into a Python dict
            data = response.json()

            # GeoJSON structure: data["features"] is a list of fire records
            features = data.get("features", [])
            print(f"✅ Total records returned: {len(features)}\n")

            # Loop through each fire and print key fields
            for i, feature in enumerate(features):
                props = feature.get("properties", {})   # the data fields
                geom  = feature.get("geometry", {})      # the coordinates

                print(f"--- FIRE #{i+1} ---")
                print(f"  Fire Name     : {props.get('FIRE_NAME', 'N/A')}")
                print(f"  County        : {props.get('COUNTY', 'N/A')}")
                print(f"  Alarm Date    : {props.get('ALARM_DATE', 'N/A')}")
                print(f"  Size (acres)  : {props.get('GIS_ACRES', 'N/A')}")
                print(f"  Cause         : {props.get('CAUSE', 'N/A')}")
                print(f"  Geometry Type : {geom.get('type', 'N/A')}")
                print()

            # Save the full response to a JSON file for reference
            output_path = "calfire_sample.json"
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)
            print(f"💾 Full response saved to: {output_path}")

        else:
            # If status code is not 200, something went wrong
            print(f"❌ API call failed! Status: {response.status_code}")
            print(f"   Response text: {response.text[:500]}")

    except requests.exceptions.Timeout:
        print("❌ ERROR: Request timed out. Check your internet connection.")
    except requests.exceptions.ConnectionError:
        print("❌ ERROR: Could not connect. Check your internet connection.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    print("\n" + "=" * 60)
    print("CAL FIRE TEST COMPLETE")
    print("=" * 60)


# ── RUN ───────────────────────────────────────────────────
if __name__ == "__main__":
    fetch_calfire_data()
