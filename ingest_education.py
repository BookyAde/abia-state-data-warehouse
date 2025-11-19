import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

# Connection to your warehouse
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

# Read the CSV we just created
df = pd.read_csv('abia_education_2024.csv')

# Add audit column
df['source_file'] = 'abia_education_2024.csv'

# Load into landing zone
df.to_sql('abia_education_raw', schema='landing', con=engine, if_exists='append', index=False)

print(f"Loaded {len(df)} rows into landing.abia_education_raw  🎉")
print("Check pgAdmin now — you will see the 10 rows!")