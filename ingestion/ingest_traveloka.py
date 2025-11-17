from utils.db import get_conn, get_or_create_channel, insert_raw_feedback
from utils.logger import info, error
from channels.traveloka import crawl_traveloka_reviews
from config.settings import TRAVELOKA_BASE_URL

def ingest_traveloka():
    """Main ingestion function - simple version"""
    conn = get_conn()
    
    try:
        info("Starting Traveloka ingestion...")
        
        # Crawl data
        hotel_name, reviews_data = crawl_traveloka_reviews(TRAVELOKA_BASE_URL, max_pages=5)
        
        if not reviews_data:
            info("No reviews data to ingest")
            return 0
            
        info(f"Processing {len(reviews_data)} reviews...")
        
        # Get channel
        channel_id = get_or_create_channel(
            conn,
            name="Traveloka",
            type_="crawl",
            base_url=TRAVELOKA_BASE_URL
        )
        
        if not channel_id:
            error("Failed to get channel")
            return 0
        
        # Transform untuk database
        transformed_reviews = []
        for review in reviews_data:
            transformed_reviews.append({
                "channel_id": channel_id,
                "author_name": review.get("author_name"),
                "rating": review.get("rating"),  # Format asli "8/10"
                "content": review.get("content"),
                "source_url": None,  # NULL untuk hindari constraint
                "review_created_at": review.get("review_created_at"),
                "metadata": review.get("metadata", {})
            })
        
        # Insert ke database
        inserted_count = insert_raw_feedback(conn, transformed_reviews)
        info(f"✅ Inserted {inserted_count} reviews into database")
        
        return inserted_count
        
    except Exception as e:
        error(f"Traveloka ingestion failed: {e}")
        return 0
        
    finally:
        conn.close()