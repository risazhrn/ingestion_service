from utils.db import get_conn, get_or_create_channel, insert_raw_feedback
from utils.logger import info, error
from channels.tripadvisor import crawl_tripadvisor_reviews
from config.settings import TRIPADVISOR_BASE_URL
from datetime import datetime
import re

def parse_tripadvisor_date(date_text):
    """Parse Tripadvisor date format ke datetime object"""
    try:
        # Contoh: "Reviewed December 25, 2023" atau "December 25, 2023"
        clean_date = date_text.replace('Reviewed', '').strip()
        
        # Try multiple date formats
        formats = [
            '%B %d, %Y',  # December 25, 2023
            '%b %d, %Y',  # Dec 25, 2023
            '%m/%d/%Y',   # 12/25/2023
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(clean_date, fmt).date()
            except ValueError:
                continue
                
        # Jika semua format gagal, return None
        error(f"❌ Cannot parse date: {date_text}")
        return None
        
    except Exception as e:
        error(f"❌ Error parsing date {date_text}: {e}")
        return None

def ingest_tripadvisor():
    """Main ingestion function for Tripadvisor - dengan debugging"""
    conn = get_conn()
    
    try:
        info("🚀 Starting Tripadvisor ingestion...")
        
        # Crawl data
        hotel_name, reviews_data = crawl_tripadvisor_reviews(TRIPADVISOR_BASE_URL, max_pages=2)
        
        # Debug: Tampilkan data yang didapat
        info(f"📊 Raw reviews data count: {len(reviews_data)}")
        for i, review in enumerate(reviews_data[:3]):  # Show first 3 for debugging
            info(f"  Review {i+1}: {review.get('author_name')} - Rating: {review.get('rating')}")
            info(f"    Date: {review.get('review_created_at')}")
            info(f"    Content preview: {review.get('content')[:50]}...")
        
        if not reviews_data:
            info("❌ No reviews data to ingest")
            return 0
            
        info(f"🔄 Processing {len(reviews_data)} reviews...")
        
        # Get channel
        channel_id = get_or_create_channel(
            conn,
            name="Tripadvisor",
            type_="crawl",
            base_url=TRIPADVISOR_BASE_URL
        )
        
        info(f"📝 Channel ID: {channel_id}")
        
        if not channel_id:
            error("❌ Failed to get channel")
            return 0
        
        # Transform untuk database
        transformed_reviews = []
        success_count = 0
        error_count = 0
        
        for review in reviews_data:
            try:
                # Parse date
                raw_date = review.get("review_created_at")
                parsed_date = parse_tripadvisor_date(raw_date)
                
                if not parsed_date:
                    error_count += 1
                    continue
                
                transformed_review = {
                    "channel_id": channel_id,
                    "author_name": review.get("author_name", "Anonymous"),
                    "rating": review.get("rating", "0/5"),
                    "content": review.get("content", ""),
                    "source_url": None,
                    "review_created_at": parsed_date,
                    "metadata": review.get("metadata", {})
                }
                
                # Validasi content tidak kosong
                if not transformed_review["content"] or not transformed_review["content"].strip():
                    warn(f"⚠️ Skipping review with empty content from {transformed_review['author_name']}")
                    error_count += 1
                    continue
                
                transformed_reviews.append(transformed_review)
                success_count += 1
                
            except Exception as e:
                error(f"❌ Error transforming review: {e}")
                error_count += 1
                continue
        
        info(f"✅ Successfully transformed: {success_count}, Failed: {error_count}")
        
        if not transformed_reviews:
            error("❌ No valid reviews to insert after transformation")
            return 0
        
        # Insert ke database
        inserted_count = insert_raw_feedback(conn, transformed_reviews)
        info(f"💾 Inserted {inserted_count} reviews into database")
        
        # Update last_ingested_at
        from utils.db import update_channel_last_ingested
        update_channel_last_ingested(conn, channel_id)
        
        return inserted_count
        
    except Exception as e:
        error(f"❌ Tripadvisor ingestion failed: {e}")
        return 0
        
    finally:
        if conn:
            conn.close()
            info("🔚 Database connection closed")
