from datetime import datetime
from channels.facebook import fetch_facebook_data
from utils.db import (
    get_conn,
    get_or_create_channel,
    insert_raw_feedback,
    update_channel_last_ingested
)
from utils.logger import info, error


def ingest_facebook():
    conn = None
    try:
        info("🚀 STARTING FACEBOOK INGESTION")

        # Step 1: Fetch data dari Facebook API
        raw_data = fetch_facebook_data(limit=3)
        if not raw_data:
            error("❌ No data fetched from Facebook API")
            return 0

        info(f"📥 Fetched {len(raw_data)} posts from Facebook")

        # Step 2: DB Connection
        conn = get_conn()
        if not conn:
            error("❌ Failed to connect to DB")
            return 0

        # Step 3: Get or create channel
        channel_id = get_or_create_channel(
            conn,
            name="Facebook",
            type_="api",
            base_url="https://graph.facebook.com"
        )
        if not channel_id:
            error("❌ Failed to create/get channel")
            return 0

        # Step 4: Transform FB data ke struktur DB
        final_data = []

        for item in raw_data:
            post_id = item["post_id"]

            for c in item["comments"]:
                created_time = c.get("created_time")
                if created_time and created_time.endswith("+0000"):
                    created_time = created_time[:-5] + "+00:00"

                try:
                    review_time = (
                        datetime.fromisoformat(created_time)
                        if created_time else datetime.utcnow()
                    )
                except Exception:
                    review_time = datetime.utcnow()
                final_data.append({
                    "channel_id": channel_id,
                    "author_name": c.get("from", {}).get("name", "Unknown"),
                    "rating": None,  # FB comment tidak ada rating
                    "content": c.get("message", ""),
                    "source_url": f"https://facebook.com/{post_id}",
                    "review_created_at": review_time,
                    "metadata": {
                        "comment_id": c.get("id"),
                        "post_id": post_id,
                        "original": c
                    }
                })

        # Step 5: Insert ke database
        inserted = insert_raw_feedback(conn, final_data)

        # Step 6: Update last ingested timestamp
        update_channel_last_ingested(conn, channel_id)

        info(f"✅ FACEBOOK INGESTION COMPLETED → {inserted} records inserted")
        return inserted

    except Exception as e:
        error(f"💥 ERROR during ingestion: {e}")
        import traceback
        error(traceback.format_exc())
    finally:
        if conn:
            conn.close()
            info("🔚 DB Connection closed")
