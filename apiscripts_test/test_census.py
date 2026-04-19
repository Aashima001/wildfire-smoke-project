"""
=============================================================
API TEST 4 — US Census Bureau (ACS 5-Year Estimates)
=============================================================
PURPOSE : Fetch demographic data for all 58 CA counties
AUTH    : Requires free API key from https://api.census.gov/data/key_signup.html
OUTPUT  : Prints county demographics and saves to census_sample.json

VARIABLE CODES EXPLAINED:
  B01001_001E = Total population
  B01002_001E = Median age
  B19013_001E = Median household income
  B18101_001E = Total population (for disability denominator)
  B27010_033E = No health insurance (under 65)
  NAME        = County name

HOW TO USE:
  1. Replace YOUR_CENSUS_API_KEY_HERE with your actual key
  2. Run: python test_census.py
=============================================================
"""

import requests
import json

# ══════════════════════════════════════════════════════════
# ⚠️  REPLACE THIS WITH YOUR ACTUAL CENSUS API KEY
CENSUS_API_KEY = "9ed34fa6e75b4b2c22cb751d565d7040039a036a"
# ══════════════════════════════════════════════════════════

# ── CONFIG ────────────────────────────────────────────────
# ACS 5-Year estimates, 2022 (most recent available)
BASE_URL = "https://api.census.gov/data/2022/acs/acs5"

# State FIPS code — 06 = California
# County FIPS — * means ALL counties in that state
STATE_FIPS = "06"


# ── STEP 1: Check what variables are available ────────────
def check_census_variables():
    """
    Optional: Check what variables the Census API provides.
    Useful to find the right variable codes for new fields.
    """
    print("\n📋 Key Census ACS5 Variable Codes:")
    variables = {
        "B01001_001E": "Total population",
        "B01002_001E": "Median age",
        "B19013_001E": "Median household income ($)",
        "B17001_002E": "Population below poverty line",
        "B18101_001E": "Total civilian noninstitutionalized population (disability denominator)",
        "B18101_004E": "Male with disability, 5-17 years",
        "B27010_033E": "No health insurance coverage (18-34)",
        "NAME"        : "Geographic name (county name)",
    }
    for code, desc in variables.items():
        print(f"   {code:20s} → {desc}")


# ── STEP 2: Fetch data for all CA counties ────────────────
def fetch_census_data():
    """
    Fetch demographic data for all 58 California counties.
    """
    print("=" * 60)
    print("TESTING: US Census Bureau ACS5 API")
    print("=" * 60)

    # Check if user has replaced the placeholder key
    if CENSUS_API_KEY == "YOUR_CENSUS_API_KEY_HERE":
        print("\n❌ STOP: You need to add your Census API key!")
        print("   Open this file and replace YOUR_CENSUS_API_KEY_HERE")
        print("   Get a free key at: https://api.census.gov/data/key_signup.html")
        return

    check_census_variables()

    # Build the API parameters
    # "get=" is a comma-separated list of variables we want
    # "for=" specifies the geography (county)
    # "in=" filters to a specific state
    params = {
        "get": ",".join([
            "NAME",           # county name
            "B01001_001E",    # total population
            "B01002_001E",    # median age
            "B19013_001E",    # median household income
            "B17001_002E",    # population below poverty line
            "B27010_033E",    # no health insurance (18-34)
        ]),
        "for"    : "county:*",          # all counties
        "in"     : f"state:{STATE_FIPS}",  # California only
        "key"    : CENSUS_API_KEY,
    }

    print(f"\n📡 Calling Census API...")
    print(f"   URL: {BASE_URL}")
    print(f"   Variables: {params['get']}")
    print(f"   Geography: All counties in California (state:{STATE_FIPS})\n")

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        print(f"HTTP Status: {response.status_code}")
        print("Content-Type:", response.headers.get("Content-Type", "N/A"))

        if response.status_code == 200:
            print("\nRaw response preview:")
            print(response.text[:500])

            try:
                data = response.json()
            except json.JSONDecodeError:
                print("❌ Response was not valid JSON.")
                return

            if not isinstance(data, list) or len(data) == 0:
                print("❌ Unexpected response format.")
                print(data)
                return

            headers = data[0]
            rows = data[1:]

            print(f"✅ Headers: {headers}")
            print(f"✅ Total counties returned: {len(rows)}\n")

            counties = []
            for row in rows:
                county_dict = dict(zip(headers, row))
                counties.append(county_dict)

            print("Sample counties (first 5):")
            print("-" * 50)
            for county in counties[:5]:
                print(f"\n  County        : {county.get('NAME', 'N/A')}")
                print(f"  Population    : {int(county.get('B01001_001E', 0) or 0):,}")
                print(f"  Median Age    : {county.get('B01002_001E', 'N/A')}")
                print(f"  Median Income : ${int(county.get('B19013_001E', 0) or 0):,}")
                print(f"  Below Poverty : {county.get('B17001_002E', 'N/A')}")
                print(f"  No Insurance  : {county.get('B27010_033E', 'N/A')}")
                print(f"  State FIPS    : {county.get('state', 'N/A')}")
                print(f"  County FIPS   : {county.get('county', 'N/A')}")

            print(f"\n{'✅' if len(rows) == 58 else '⚠️'} Expected 58 counties, got {len(rows)}")

            output_path = "census_sample.json"
            with open(output_path, "w") as f:
                json.dump(counties, f, indent=2)
            print(f"\n💾 Full data saved to: {output_path}")

        elif response.status_code == 400:
            print("❌ Bad request. Check variable codes or parameters.")
            print(response.text[:500])

        elif response.status_code == 401:
            print("❌ Invalid API key.")
            print(response.text[:500])

        else:
            print(f"❌ Error {response.status_code}: {response.text[:500]}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    print("\n" + "=" * 60)
    print("CENSUS TEST COMPLETE")
    print("=" * 60)


# ── RUN ───────────────────────────────────────────────────
if __name__ == "__main__":
    fetch_census_data()
