# Wildfire & Smoke Project

Real-time California wildfire, air quality, and vulnerability analytics warehouse.

## Team
- Ishan Shah
- Aashima Taneja  
- Shashank Ranjan

## Architecture
CAL FIRE + EPA AirNow + NOAA + US Census → Apache Airflow → Amazon S3 → Snowflake

## Data Sources
- CAL FIRE incident API
- EPA AirNow AQI API
- NOAA Weather API
- US Census Bureau ACS 5-year estimates

## Pipeline
11-task Airflow DAG covering data ingestion, ETL, 
staging, dimension tables, and fact table with composite risk scoring.

## Setup
1. Clone this repo
2. Copy `.env.example` to `.env` and fill in your API keys
3. Run `docker-compose up airflow-init`
4. Run `docker-compose up -d webserver scheduler`
5. Trigger the DAG at localhost:8080
