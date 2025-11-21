import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "omni_feedback_db")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_PLACE_ID = os.getenv("GOOGLE_PLACE_ID")

GOOGLE_BASE_URL = "https://maps.googleapis.com/maps/api/place/details/json"
TRAVELOKA_BASE_URL = os.getenv("TRAVELOKA_BASE_URL")
TRIPADVISOR_BASE_URL = os.getenv("TRIPADVISOR_BASE_URL")

FB_BASE_URL = os.getenv("FB_BASE_URL")
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")