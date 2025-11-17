import requests
from datetime import datetime
from utils.logger import info, error
from config.settings import GOOGLE_API_KEY, GOOGLE_PLACE_ID, GOOGLE_BASE_URL
from utils.db import get_conn, get_or_create_channel, insert_raw_feedback, update_channel_last_ingested
from channels.google_maps import fetch_google_reviews, process_google_reviews

def ingest_google():
    """
    Main ingestion function untuk Google Maps reviews
    """
    conn = None
    
    try:
        info("=" * 50)
        info("🚀 STARTING GOOGLE MAPS INGESTION")
        info("=" * 50)
        
        # Step 1: Fetch data dari Google API
        info("📡 Step 1: Fetching data from Google Places API...")
        place, raw_reviews = fetch_google_reviews()
        
        if place is None or not raw_reviews:
            error("❌ No data fetched from Google API")
            return 0
            
        info(f"📊 Fetched {len(raw_reviews)} raw reviews")

        # Step 2: Setup database connection
        info("💾 Step 2: Setting up database connection...")
        conn = get_conn()
        if not conn:
            error("❌ Failed to establish database connection")
            return 0

        # Step 3: Get or create channel
        info("🏷️ Step 3: Setting up channel...")
        channel_id = get_or_create_channel(
            conn, 
            name="Google Maps", 
            type_="api", 
            base_url=GOOGLE_BASE_URL
        )
        
        if not channel_id:
            error("❌ Failed to get or create channel")
            return 0
            
        info(f"✅ Channel ID: {channel_id}")

        # Step 4: Process reviews dengan auto-translate
        info("🔄 Step 4: Processing reviews with auto-translation...")
        processed_reviews = process_google_reviews(raw_reviews)
        
        if not processed_reviews:
            error("❌ No reviews after processing")
            return 0
            
        info(f"📝 Processed {len(processed_reviews)} reviews")

        # Step 5: Transform untuk database
        info("🗃️ Step 5: Transforming data for database...")
        transformed_reviews = []
        for review in processed_reviews:
            transformed_reviews.append({
                "channel_id": channel_id,
                "author_name": review.get("author_name"),
                "rating": review.get("rating"),
                "content": review.get("content"),
                "source_url": review.get("source_url"),
                "review_created_at": review.get("review_created_at"),
                "metadata": review.get("metadata", {})
            })

        # Step 6: Insert ke database
        info("💽 Step 6: Inserting into database...")
        inserted_count = insert_raw_feedback(conn, transformed_reviews)
        
        # Step 7: Update channel last ingested
        info("🕒 Step 7: Updating channel timestamp...")
        updated = update_channel_last_ingested(conn, channel_id)
        if updated:
            info("✅ Channel timestamp updated successfully")
        else:
            error("❌ Failed to update channel timestamp")

        info("=" * 50)
        info(f"✅ GOOGLE MAPS INGESTION COMPLETED")
        info(f"📈 Inserted: {inserted_count} reviews")
        info("=" * 50)
        
        return inserted_count
        
    except Exception as e:
        error("💥 GOOGLE MAPS INGESTION FAILED")
        error(f"Error: {e}")
        import traceback
        error(f"Trace: {traceback.format_exc()}")
        return 0
        
    finally:
        if conn:
            conn.close()
            info("🔚 Database connection closed")