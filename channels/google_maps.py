import requests
from datetime import datetime
from utils.logger import info, error
from config.settings import GOOGLE_API_KEY, GOOGLE_PLACE_ID, GOOGLE_BASE_URL
from utils.db import get_or_create_channel, insert_raw_feedback, update_channel_last_ingested, get_conn


def fetch_google_reviews():
    params = {
        'place_id': GOOGLE_PLACE_ID,
        'fields': 'name,rating,reviews,user_ratings_total,formatted_address',
        'key': GOOGLE_API_KEY,
        'language': 'id'
    }

    response = requests.get(GOOGLE_BASE_URL, params=params)
    data = response.json()

    if data.get("status") != "OK":
        error(f"Google API Error: {data}")
        return None, []

    place = data.get("result", {})
    reviews = place.get("reviews", [])
    return place, reviews


def ingest_google():
    conn = get_conn()
    info("Connected to DB.")

    place, reviews = fetch_google_reviews()
    if place is None:
        conn.close()
        return

    info(f"Data fetched from Google API. Total reviews fetched: {len(reviews)}")

    # Register channel
    channel_id = get_or_create_channel(
        conn,
        name="Google Maps",
        type_="api",
        base_url="https://maps.google.com"
    )

    if not channel_id:
        error("Failed to get or create channel. Aborting ingestion.")
        conn.close()
        return

    info(f"Using channel_id: {channel_id}")

    # Parse reviews
    items = []
    for r in reviews:
        review_time = datetime.fromtimestamp(r["time"]) if r.get("time") else None
        items.append({
            "channel_id": channel_id,
            "author_name": r.get("author_name"),
            "rating": r.get("rating"),
            "content": r.get("text"),
            "source_url": r.get("author_url"),
            "review_created_at": review_time,
            "metadata": {
                "language": r.get("language"),
                "original_language": r.get("original_language"),
                "profile_photo_url": r.get("profile_photo_url"),
                "relative_time": r.get("relative_time_description"),
                "translated": r.get("translated"),
            }
        })

    info(f"Parsed {len(items)} reviews to insert.")

    inserted = insert_raw_feedback(conn, items)
    info(f"[SUCCESS] Inserted {inserted} rows into raw_feedback.")

    # Update last_ingested_at
    updated = update_channel_last_ingested(conn, channel_id)
    if updated:
        info("[INFO] last_ingested_at updated successfully.")
    else:
        error("[ERROR] Failed to update last_ingested_at.")

    conn.close()
    info("Database connection closed.")
