import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()  # loads the .env file

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(connection_string)

try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT current_database(), version();"))
        for row in result:
            print("✅ Connected successfully!")
            print(f"   Database → {row[0]}")
            print(f"   PostgreSQL → {row[1][:60]}...")
    print("\nWe are 100% ready to build the data warehouse! 🔥")
except Exception as e:
    print("❌ Connection failed:")
    print(e)