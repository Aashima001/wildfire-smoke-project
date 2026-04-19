# Wildfire & Smoke Compass — API Test Scripts
## Aashima Taneja | Week 1 Deliverable

---

## 📁 Files in this folder

| File | Purpose |
|------|---------|
| `test_calfire.py` | Tests CAL FIRE ArcGIS API — no key needed |
| `test_airnow.py`  | Tests EPA AirNow AQI API — needs your key |
| `test_noaa.py`    | Tests NOAA Weather API — needs your email |
| `test_census.py`  | Tests US Census ACS5 API — needs your key |
| `run_all_tests.py`| Runs all 4 tests in sequence |

---

## ▶️ How to Run

### Step 1 — Install dependencies
```bash
pip install requests pandas
```

### Step 2 — Add your API keys
- Open `test_airnow.py` → replace `YOUR_AIRNOW_API_KEY_HERE`
- Open `test_census.py` → replace `YOUR_CENSUS_API_KEY_HERE`
- Open `test_noaa.py`   → replace `your_email@sjsu.edu` in User-Agent

### Step 3 — Run individual tests
```bash
python test_calfire.py    # Test 1 (no key needed — run this first!)
python test_airnow.py     # Test 2
python test_noaa.py       # Test 3
python test_census.py     # Test 4
```

### Step 4 — Or run all at once
```bash
python run_all_tests.py
```

---

## ✅ Expected Output Files
After running, you should see these JSON files created:
- `calfire_sample.json` — 5 recent CA wildfire records
- `airnow_sample.json`  — AQI readings for 4 CA counties
- `noaa_sample.json`    — Weather observations for 5 stations
- `census_sample.json`  — Demographics for all 58 CA counties

---

## 🔑 API Keys Registration Links
| API | Link | Wait time |
|-----|------|-----------|
| EPA AirNow | https://docs.airnowapi.org/account/request/ | Minutes |
| US Census  | https://api.census.gov/data/key_signup.html | Instant |
| NOAA/NWS   | No key — just add email to User-Agent header | N/A |
| CAL FIRE   | No key — public ArcGIS API | N/A |

---

## ❗ Troubleshooting

| Error | Fix |
|-------|-----|
| `401 Unauthorized` | API key is wrong or not activated yet |
| `ConnectionError` | Check internet connection |
| `Timeout` | Try again — API may be slow |
| `No data returned` | Normal for some locations with no nearby stations |
| Empty JSON `[]` | AirNow has no station within 25 miles — try distance=50 |
