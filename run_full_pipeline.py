# build_abia_warehouse.py
# ONE SCRIPT TO BUILD THE ENTIRE ABIA STATE MINI DATA WAREHOUSE
# Just run: python build_abia_warehouse.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ==================== 1. CONNECTION ====================
user = os.getenv('DB_USER', 'postgres')
host = os.getenv('DB_HOST', 'localhost')
port = os.getenv('DB_PORT', '5432')
db   = os.getenv('DB_NAME', 'abia_data_warehouse')

engine_url = f"postgresql://{user}@{host}:{port}/{db}"
engine = create_engine(engine_url)
print("Connected to PostgreSQL ✅\n")

# ==================== 2. RECREATE SCHEMAS (fresh start every time) ====================
with engine.connect() as conn:
    conn.execute(text("DROP SCHEMA IF EXISTS landing CASCADE;"))
    conn.execute(text("DROP SCHEMA IF EXISTS staging CASCADE;"))
    conn.execute(text("DROP SCHEMA IF EXISTS dwh     CASCADE;"))

    conn.execute(text("CREATE SCHEMA landing;"))
    conn.execute(text("CREATE SCHEMA staging;"))
    conn.execute(text("CREATE SCHEMA dwh;"))
    conn.commit()
print("Schemas recreated (landing → staging → dwh) ✅\n")

# ==================== 3. SAMPLE DATA (embedded so you never lose it) ====================
data = {}

data['education'] = pd.DataFrame([
    {"lga":"Aba North","school_name":"Community Primary School Aba","school_type":"Primary","ownership":"Public","enrollment_total":850,"enrollment_male":420,"enrollment_female":430,"teachers_total":28,"teachers_male":8,"teachers_female":20,"classrooms":18,"year":2024},
    {"lga":"Aba South","school_name":"Abayi Girls Secondary School","school_type":"Senior Secondary","ownership":"Public","enrollment_total":720,"enrollment_male":0,"enrollment_female":720,"teachers_total":35,"teachers_male":12,"teachers_female":23,"classrooms":20,"year":2024},
    {"lga":"Umuahia North","school_name":"Government College Umuahia","school_type":"Senior Secondary","ownership":"Public","enrollment_total":1100,"enrollment_male":650,"enrollment_female":450,"teachers_total":52,"teachers_male":30,"teachers_female":22,"classrooms":30,"year":2024},
    {"lga":"Ohafia","school_name":"Ohafia High School","school_type":"Senior Secondary","ownership":"Public","enrollment_total":890,"enrollment_male":500,"enrollment_female":390,"teachers_total":42,"teachers_male":25,"teachers_female":17,"classrooms":25,"year":2024},
    {"lga":"Osisioma","school_name":"Osisioma Model School","school_type":"Primary","ownership":"Public","enrollment_total":680,"enrollment_male":340,"enrollment_female":340,"teachers_total":22,"teachers_male":6,"teachers_female":16,"classrooms":15,"year":2024},
], columns=["lga","school_name","school_type","ownership","enrollment_total","enrollment_male","enrollment_female","teachers_total","teachers_male","teachers_female","classrooms","year"])

# (I fixed the typos above — here is the clean version you can copy)
# Use this proper list instead (copy from previous messages or this corrected one):

education_data = [
    ("Aba North", "Community Primary School Aba", "Primary", "Public", 850, 420, 430, 28, 8, 20, 18, 2024),
    ("Aba South", "Abayi Girls Secondary School", "Senior Secondary", "Public", 720, 0, 720, 35, 12, 23, 20, 2024),
    ("Umuahia North", "Government College Umuahia", "Senior Secondary", "Public", 1100, 650, 450, 52, 30, 22, 30, 2024),
    ("Umuahia South", "Model Primary School Umudike", "Primary", "Public", 480, 240, 240, 18, 5, 13, 12, 2024),
    ("Ohafia", "Ohafia High School", "Senior Secondary", "Public", 890, 500, 390, 42, 25, 17, 25, 2024),
    ("Osisioma", "Osisioma Model School", "Primary", "Public", 680, 340, 340, 22, 6, 16, 15, 2024),
]
df_edu = pd.DataFrame(education_data, columns=["lga","school_name","school_type","ownership","enrollment_total","enrollment_male","enrollment_female","teachers_total","teachers_male","teachers_female","classrooms","year"])

df_health = pd.DataFrame([
    ("Aba North", "Aba General Hospital", "Hospital", "Public", 12, 45, 120, 2024),
    ("Aba South", "Abayi Primary Health Centre", "PHC", "Public", 2, 8, 10, 2024),
    ("Umuahia North", "UMC Hospital Umuahia", "Hospital", "Public", 15, 60, 200, 2024),
    ("Ohafia", "Ohafia General Hospital", "Hospital", "Public", 8, 35, 80, 2024),
    ("Osisioma", "Green Life Hospital", "Hospital", "Private", 10, 30, 60, 2024),
], columns=["lga","facility_name","facility_type","ownership","doctors","nurses","bed_capacity","year"])

df_revenue = pd.DataFrame([
    ("Aba North", "January", 450_000_000, 1_200_000_000, 1_650_000_000, 2024),
    ("Aba South", "January", 520_000_000, 1_150_000_000, 1_670_000_000, 2024),
    ("Umuahia North", "January", 280_000_000, 1_400_000_000, 1_680_000_000, 2024),
    ("Ohafia", "January", 180_000_000, 950_000_000, 1_130_000_000, 2024),
], columns=["lga","month","igr_collected","federal_allocation","total_revenue","year"])

# ==================== 4. LOAD TO LANDING ====================
df_edu.to_sql('abia_education_raw', schema='landing', con=engine, if_exists='replace', index=False)
df_health.to_sql('abia_health_raw', schema='landing', con=engine, if_exists='replace', index=False)
df_revenue.to_sql('abia_revenue_raw', schema='landing', con=engine, if_exists='replace', index=False)
print("Raw data loaded into landing zone ✅\n")

# ==================== 5. BUILD STAR SCHEMA (dwh) ====================
with engine.connect() as conn:
    conn.execute(text("""
        -- Dimension: LGA
        CREATE TABLE dwh.dim_lga (
            lga_key SERIAL PRIMARY KEY,
            lga_name VARCHAR(50) UNIQUE NOT NULL
        );

        -- Dimension: Date (simple year-month)
        CREATE TABLE dwh.dim_date (
            date_key SERIAL PRIMARY KEY,
            year INT,
            month VARCHAR(20),
            year_month VARCHAR(10) UNIQUE
        );

        -- Fact table combining all metrics
        CREATE TABLE dwh.fact_abia_metrics (
            fact_id SERIAL PRIMARY KEY,
            lga_key INT REFERENCES dwh.dim_lga(lga_key),
            date_key INT REFERENCES dwh.dim_date(date_key),
            enrollment_total INT,
            teachers_total INT,
            doctors INT,
            nurses INT,
            total_revenue BIGINT
        );
    """))
    conn.commit()

# Populate dimensions
with engine.connect() as conn:
    # dim_lga
    conn.execute(text("INSERT INTO dwh.dim_lga (lga_name) SELECT DISTINCT lga FROM landing.abia_education_raw ON CONFLICT DO NOTHING;"))
    conn.execute(text("INSERT INTO dwh.dim_lga (lga_name) SELECT DISTINCT lga FROM landing.abia_health_raw ON CONFLICT DO NOTHING;"))
    conn.execute(text("INSERT INTO dwh.dim_lga (lga_name) SELECT DISTINCT lga FROM landing.abia_revenue_raw ON CONFLICT DO NOTHING;"))

    # dim_date — just 2024 January for now, but expandable
    conn.execute(text("INSERT INTO dwh.dim_date (year, month, year_month) VALUES (2024, 'January', '2024-01') ON CONFLICT DO NOTHING;"))
    conn.commit()

# Load fact table
with engine.connect() as conn:
    conn.execute(text("""
        INSERT INTO dwh.fact_abia_metrics (
            lga_key, date_key, enrollment_total, teachers_total, doctors, nurses, total_revenue
        )
        SELECT 
            l.lga_key,
            d.date_key,
            e.enrollment_total,
            e.teachers_total,
            h.doctors,
            h.nurses,
            r.total_revenue
        FROM landing.abia_education_raw e
        JOIN dwh.dim_lga l ON e.lga = l.lga_name
        JOIN landing.abia_health_raw h ON e.lga = h.lga
        JOIN landing.abia_revenue_raw r ON e.lga = r.lga
        CROSS JOIN dwh.dim_date d
        WHERE d.year_month = '2024-01';
    """))
    conn.commit()
print("Star schema built and populated in dwh ✅\n")

# ==================== 6. BEAUTIFUL REPORT QUERIES (run in pgAdmin) ====================
reports = """
-- REPORT 1: Top 5 LGAs by Total Students
SELECT l.lga_name, SUM(f.enrollment_total) AS total_students
FROM dwh.fact_abia_metrics f
JOIN dwh.dim_lga l ON f.lga_key = l.lga_key
GROUP BY l.lga_name ORDER BY total_students DESC LIMIT 5;

-- REPORT 2: Pupil-Teacher Ratio by LGA
SELECT l.lga_name,
       ROUND(AVG(f.enrollment_total::FLOAT / NULLIF(f.teachers_total,0)),  ),1) AS pupil_teacher_ratio
FROM dwh.fact_abia_metrics f
JOIN dwh.dim_lga l ON f.lga_key = l.lga_key
GROUP BY l.lga_name ORDER BY pupil_teacher_ratio DESC;

-- REPORT 3: Health Staff vs Revenue (who needs more funding?)
SELECT l.lga_name, 
       COALESCE(f.doctors,0) + COALESCE(f.nurses,0) AS health_staff,
       f.total_revenue / 1e9 AS revenue_billions
FROM dwh.fact_abia_metrics f
JOIN dwh.dim_lga l ON f.lga_key = l.lga_key
ORDER BY health_staff ASC;  -- poorest staffed at the top
"""

print("YOUR ABIA STATE DATA WAREHOUSE IS NOW 100% COMPLETE! 🎉\n")
print("Open pgAdmin → abia_data_warehouse → Schemas → dwh → Tables")
print("Then run these reports (copy-paste):")
print("-" * 60)
print(reports)
print("-" * 60)
print("You can now defend this project with pride. Well done!")