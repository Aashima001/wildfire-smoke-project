"""
=============================================================
RUN ALL API TESTS — Wildfire & Smoke Compass
=============================================================
Run this file to test all 4 APIs at once.

BEFORE RUNNING:
  1. Open test_airnow.py  → add your AirNow API key
  2. Open test_census.py  → add your Census API key
  3. Open test_noaa.py    → add your email in the User-Agent
  4. Then run: python run_all_tests.py
=============================================================
"""

import subprocess
import sys

scripts = [
    ("CAL FIRE (ArcGIS)", "test_calfire.py"),
    ("EPA AirNow",        "test_airnow.py"),
    ("NOAA / NWS",        "test_noaa.py"),
    ("US Census",         "test_census.py"),
]

print("\n" + "█" * 60)
print("  WILDFIRE & SMOKE COMPASS — API TEST SUITE")
print("█" * 60)

for name, script in scripts:
    print(f"\n\n{'━'*60}")
    print(f"  RUNNING: {name}")
    print(f"{'━'*60}")
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        print(f"⚠️  {name} exited with errors (check output above)")

print("\n\n" + "█" * 60)
print("  ALL TESTS COMPLETE")
print("  Check these output files:")
print("    📄 calfire_sample.json")
print("    📄 airnow_sample.json")
print("    📄 noaa_sample.json")
print("    📄 census_sample.json")
print("█" * 60 + "\n")
