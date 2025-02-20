import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB_SUPERSET = os.getenv("POSTGRES_DB")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres:5432/{POSTGRES_DB_SUPERSET}"
)
SECRET_KEY = os.getenv("SUPERSET")

# Enable guest user
PUBLIC_ROLE_LIKE = "Gamma"
