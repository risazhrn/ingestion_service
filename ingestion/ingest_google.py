from datetime import datetime
from utils.logger import info, error
from utils.db import (
    get_conn,
    get_or_create_channel,
    insert_raw_feedback,
    update_channel_last_ingested,
)
from channels.google_maps import fetch_google_reviews, process_google_reviews
from config.settings import GOOGLE_BASE_URL


def ingest_google():
    """
    Main ingestion pipeline: fetch → process → store → update timestamp.
    """
    conn = None

    try:
        info("=" * 60)
        info("🚀 STARTING GOOGLE MAPS INGESTION")
        info("=" * 60)

        # ---------------------------------------------------------------------
        # 1. FETCH GOOGLE API
        # ---------------------------------------------------------------------
        place, raw_reviews = fetch_google_reviews()
        if not place or not raw_reviews:
            error("❌ No reviews fetched from Google API")
            return 0

        info(f"📊 Raw reviews fetched: {len(raw_reviews)}")

        # ---------------------------------------------------------------------
        # 2. DB CONNECTION
        # ---------------------------------------------------------------------
        conn = get_conn()
        if not conn:
            error("❌ Failed to connect to database")
            return 0
        info("💾 Connected to database")

        # ---------------------------------------------------------------------
        # 3. CHANNEL SETUP
        # ---------------------------------------------------------------------
        channel_id = get_or_create_channel(
            conn,
            name="Google Maps",
            type_="api",
            base_url=GOOGLE_BASE_URL,
        )

        if not channel_id:
            error("❌ Failed to create/find channel")
            return 0

        info(f"🏷️ Channel ID: {channel_id}")

        # ---------------------------------------------------------------------
        # 4. PROCESS REVIEWS
        # ---------------------------------------------------------------------
        processed = process_google_reviews(raw_reviews)
        if not processed:
            error("❌ No valid reviews after processing")
            return 0

        info(f"📝 Reviews ready for DB: {len(processed)}")

        # ---------------------------------------------------------------------
        # 5. TRANSFORM & INSERT
        # ---------------------------------------------------------------------
        transformed = []
        for r in processed:
            transformed.append({
                "channel_id": channel_id,
                "author_name": r["author_name"],
                "rating": r["rating"],
                "content": r["content"],
                "source_url": r["source_url"],
                "review_created_at": r["review_created_at"],
                "metadata": r["metadata"],
            })

        inserted_count = insert_raw_feedback(conn, transformed)

        # ---------------------------------------------------------------------
        # 6. UPDATE LAST INGESTED
        # ---------------------------------------------------------------------
        if update_channel_last_ingested(conn, channel_id):
            info("🕒 Channel last_ingested_at updated")
        else:
            error("❌ Failed to update last_ingested_at")

        info("=" * 60)
        info(f"✅ INGESTION COMPLETED — Inserted {inserted_count} reviews")
        info("=" * 60)

        return inserted_count

    except Exception as e:
        error("💥 INGESTION FAILED")
        error(f"Error: {e}")
        import traceback
        error(traceback.format_exc())
        return 0

    finally:
        if conn:
            conn.close()
            info("🔚 DB connection closed")
