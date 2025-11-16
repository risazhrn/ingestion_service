import requests
from datetime import datetime

from config.settings import (
    GOOGLE_API_KEY,
    GOOGLE_PLACE_ID,
    GOOGLE_BASE_URL
)

from utils.db import (
    get_conn,
    get_or_create_channel,
    insert_raw_feedback
)

def ingest_google():
    conn = get_conn()
    print("[INFO] Connected to DB.")

    try:
        # ================================
        # 1. Fetch data dari Google API
        # ================================
        params = {
            "place_id": GOOGLE_PLACE_ID,
            "fields": "name,rating,reviews,formatted_address,user_ratings_total",
            "key": GOOGLE_API_KEY,
            "language": "id"
        }

        response = requests.get(GOOGLE_BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        print("[INFO] Data fetched from Google API.")

        if data.get("status") != "OK":
            raise ValueError(f"Google API error: {data.get('status')} | {data.get('error_message')}")

        result = data.get("result", {})
        reviews = result.get("reviews", [])

        print(f"[INFO] Total reviews fetched: {len(reviews)}")

        # ================================
        # 2. Get or create channel
        # ================================
        channel_id = get_or_create_channel(
            conn,
            name="Google Reviews",
            type_="api",
            base_url=GOOGLE_BASE_URL
        )

        print(f"[INFO] Using channel_id: {channel_id}")

        # ================================
        # 3. Parse reviews
        # ================================
        parsed = []

        for r in reviews:
            parsed.append({
                "channel_id": channel_id,
                "author_name": r.get("author_name"),
                "rating": r.get("rating"),
                "content": r.get("text"),
                "source_url": r.get("author_url"),
                "review_created_at": datetime.fromtimestamp(r.get("time")),
                "metadata": {
                    "language": r.get("language"),
                    "profile_photo_url": r.get("profile_photo_url"),
                    "relative_time_description": r.get("relative_time_description"),
                    "original_language": r.get("original_language"),
                    "translated": r.get("translated")
                }
            })

        print(f"[INFO] Parsed {len(parsed)} reviews to insert.")

        # ================================
        # 4. Insert into DB
        # ================================
        count = insert_raw_feedback(conn, parsed)
        print(f"[SUCCESS] Inserted {count} rows into raw_feedback.")

    except Exception as e:
        print("[ERROR] Google ingestion failed:")
        print(str(e))

    finally:
        conn.close()
        print("[INFO] Database connection closed.")
