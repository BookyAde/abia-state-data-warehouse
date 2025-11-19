import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(f"postgresql://{os.getenv('DB_USER')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

# 1. Pull raw data
raw = pd.read_sql("SELECT * FROM landing.abia_education_raw", engine)

print(f"Raw rows before cleaning: {len(raw)}")

# 2. Basic cleaning
df = raw.copy()
df = df.drop_duplicates()  # remove accidental full duplicates
df['lga'] = df['lga'].str.strip().str.title()
df['school_name'] = df['school_name'].str.strip()
df['school_type'] = df['school_type'].str.strip()
df['ownership'] = df['ownership'].str.strip()

# 3. Calculate KPIs
df['student_teacher_ratio'] = round(df['enrollment_total'] / df['teachers_total'].replace(0, 1), 2)
df['female_enrollment_pct'] = round(df['enrollment_female'] / df['enrollment_total'].replace(0, 1) * 100, 2)
df['female_teacher_pct']    = round(df['teachers_female'] / df['teachers_total'].replace(0, 1) * 100, 2)

# 4. Load into staging (replace so we always have clean version)
df.to_sql('abia_education_clean', schema='staging', con=engine, if_exists='replace', index=False)

print(f"✅ Transformation complete! {len(df)} clean rows now in staging.abia_education_clean")
print("\nSample of clean data:")
print(df[['lga', 'school_name', 'enrollment_total', 'student_teacher_ratio', 'female_enrollment_pct']].head())