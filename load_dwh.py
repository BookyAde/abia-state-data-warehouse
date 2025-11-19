import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

# 1. Load clean data
df = pd.read_sql("SELECT * FROM staging.abia_education_clean", engine)
print(f"Starting with {len(df)} clean records...")

# 2. TRUNCATE ALL TABLES SAFELY
with engine.connect() as conn:
    conn.execute(text("""
        TRUNCATE TABLE 
            dwh.fact_education,
            dwh.dim_lga,
            dwh.dim_school,
            dwh.dim_date
        RESTART IDENTITY CASCADE;
    """))
    conn.commit()

print("Old data cleared – fresh reload!")

# 3. Populate dimensions
lga_df = pd.DataFrame(df['lga'].unique(), columns=['lga_name'])
lga_df.to_sql('dim_lga', schema='dwh', con=engine, if_exists='append', index=False)

date_df = pd.DataFrame({'year': [2024]})
date_df.to_sql('dim_date', schema='dwh', con=engine, if_exists='append', index=False)

school_df = df[['school_name', 'school_type', 'ownership', 'lga']].drop_duplicates()
school_df = school_df.rename(columns={'lga': 'lga_name'})
school_df.to_sql('dim_school', schema='dwh', con=engine, if_exists='append', index=False)

# 4. Load keys
dim_lga = pd.read_sql("SELECT lga_key, lga_name FROM dwh.dim_lga", engine)
dim_school = pd.read_sql("SELECT school_key, school_name FROM dwh.dim_school", engine)
dim_date = pd.read_sql("SELECT date_key, year FROM dwh.dim_date", engine)

# 5. Build & load fact
fact = df.merge(dim_school, on='school_name') \
         .merge(dim_lga, left_on='lga', right_on='lga_name') \
         .merge(dim_date, on='year')

fact_table = fact[[
    'school_key', 'lga_key', 'date_key',
    'enrollment_total', 'enrollment_male', 'enrollment_female', 'female_enrollment_pct',
    'teachers_total', 'teachers_male', 'teachers_female', 'female_teacher_pct',
    'classrooms', 'student_teacher_ratio'
]]

fact_table.to_sql('fact_education', schema='dwh', con=engine, if_exists='append', index=False)

print("✅ ABIA STATE DATA WAREHOUSE 100% COMPLETE!")
print(f"   → {len(dim_lga)} LGAs | {len(dim_school)} Schools | {len(fact_table)} Fact rows")
print("\nGo run the queries – you now have real analytics ready for presentation!")
