import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os 
from sqlalchemy.orm import sessionmaker

env_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(env_path)



user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
DATABASE_URL = (
    f"postgresql://{user}:{password}@{db_host}:{db_port}/{db_name}"
    "?sslmode=require"
)
engine = create_engine(DATABASE_URL, echo=True, future= True)
print(engine)

try:
    with engine.connect() as connection:
        print("Connection to the database was successful!")
except Exception as e:
    print(f"An error occurred while connecting to the database: {e}")


SessionLocal = sessionmaker(autocommit= False, autoflush= False, bind= engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


