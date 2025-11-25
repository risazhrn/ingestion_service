from utils.db import get_conn, get_or_create_channel, insert_raw_feedback, update_channel_last_ingested
from utils.logger import info, error, warn
from channels.traveloka import crawl_traveloka_reviews
from config.settings import TRAVELOKA_BASE_URL


def ingest_traveloka(max_pages=5):
    conn = get_conn()

    try:
        info("Starting Traveloka ingestion")

        info(f"Crawling data from: {TRAVELOKA_BASE_URL}")
        hotel_name, reviews_data = crawl_traveloka_reviews(TRAVELOKA_BASE_URL, max_pages=max_pages)

        info(f"Hotel Name: {hotel_name}, Reviews Count: {len(reviews_data)}")
        if not reviews_data:
            error("No reviews data to ingest")
            return 0

        info("Getting or creating Traveloka channel")
        channel_id = get_or_create_channel(conn, name="Traveloka", type_="crawl", base_url=TRAVELOKA_BASE_URL)
        if not channel_id:
            error("Failed to get or create channel")
            return 0
        info(f"Channel ID: {channel_id}")

        transformed_reviews = []
        valid_count = 0
        invalid_count = 0

        for review in reviews_data:
            author_name = review.get("author_name", "").strip()
            content = review.get("content", "").strip()
            rating = review.get("rating")
            review_date = review.get("review_created_at")
            metadata = review.get("metadata", {})

            if not author_name or not content or rating is None or not review_date:
                warn(f"Skipping invalid review: {author_name}")
                invalid_count += 1
                continue

            transformed_reviews.append({
                "channel_id": channel_id,
                "author_name": author_name,
                "rating": rating,
                "content": content,
                "source_url": TRAVELOKA_BASE_URL,
                "review_created_at": review_date,
                "metadata": {
                    **metadata,
                    "hotel_name": hotel_name,
                    "source_type": "crawl"
                }
            })
            valid_count += 1

        info(f"Transformation Summary: {valid_count} valid, {invalid_count} invalid")
        if not transformed_reviews:
            error("No valid reviews after transformation")
            return 0

        info("Inserting data into database")
        inserted_count = insert_raw_feedback(conn, transformed_reviews)

        update_channel_last_ingested(conn, channel_id)

        info(f"Traveloka ingestion completed - {inserted_count} records inserted")
        return inserted_count

    except Exception as e:
        error(f"Traveloka ingestion failed: {e}")
        import traceback
        error(f"Stack trace: {traceback.format_exc()}")
        return 0

    finally:
        if conn:
            conn.close()
            info("Database connection closed")


if __name__ == "__main__":
    inserted = ingest_traveloka()
    print(f"Inserted {inserted} records")