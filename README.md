# Abia State Integrated Data Warehouse 🏛️

A mini enterprise-level data warehouse that consolidates Education, Health, and Revenue data for all Local Government Areas in Abia State, Nigeria.

## Features
- 3-layer architecture: Landing → Staging → Data Warehouse (Star Schema)
- Fully automated Python ETL pipeline
- One-click build: just run `python build_abia_warehouse.py`
- Ready-to-use analytical queries for government decision-making

## How to Run
1. Install requirements: `pip install pandas sqlalchemy psycopg2-binary python-dotenv`
2. Make sure PostgreSQL is running and you have a database called `abia_data_warehouse`
3. Run: `python build_abia_warehouse.py`

The entire warehouse builds in < 10 seconds! 🚀

## Business Impact
Helps Abia State Government with:
- Resource allocation
- Budget planning
- Performance monitoring across LGAs