from datetime import datetime
from utils.db import (
    get_conn,
    get_or_create_channel,
    insert_raw_feedback,
    update_channel_last_ingested
)
from utils.logger import info, error
from channels.facebook import fetch_facebook_data
from config.settings import FB_BASE_URL

class FacebookIngestor:
    def __init__(self):
        self.channel_name = "Facebook"
        self.channel_type = "api"
        self.base_url = FB_BASE_URL
    
    def _transform_comment_date(self, date_string):
        """
        Transform Facebook date format ke Python datetime
        
        Args:
            date_string (str): Date string dari Facebook API
            
        Returns:
            datetime: Python datetime object
        """
        try:
            # Handle Facebook's ISO format dengan timezone
            if date_string and date_string.endswith("+0000"):
                date_string = date_string[:-5] + "+00:00"
            
            return datetime.fromisoformat(date_string) if date_string else datetime.utcnow()
        except Exception as e:
            error(f"❌ Error transforming date {date_string}: {e}")
            return datetime.utcnow()
    
    def _transform_facebook_data(self, raw_data, channel_id):
        """
        Transform data Facebook ke format database
        
        Args:
            raw_data (list): Raw data dari Facebook API
            channel_id (int): ID channel Facebook di database
            
        Returns:
            list: Transformed data siap untuk database
        """
        transformed_data = []
        
        for post in raw_data:
            post_id = post["post_id"]
            
            for comment in post["comments"]:
                # Transform comment data
                transformed_comment = {
                    "channel_id": channel_id,
                    "author_name": comment.get("from", {}).get("name", "Unknown User"),
                    "rating": None,  # Facebook comments don't have ratings
                    "content": comment.get("message", ""),
                    "source_url": f"https://facebook.com/{post_id}",
                    "review_created_at": self._transform_comment_date(comment.get("created_time")),
                    "metadata": {
                        "comment_id": comment.get("id"),
                        "post_id": post_id,
                        "post_message": post.get("message", "")[:200],  # Truncate long posts
                        "original_data": comment  # Keep original for reference
                    }
                }
                
                # Only include comments with actual content
                if transformed_comment["content"].strip():
                    transformed_data.append(transformed_comment)
                else:
                    info(f"⚠️ Skipping empty comment from {transformed_comment['author_name']}")
        
        return transformed_data
    
    def ingest(self, post_limit=3):
        """
        Main ingestion process untuk Facebook data
        
        Args:
            post_limit (int): Jumlah postingan yang akan di-process
            
        Returns:
            int: Jumlah records yang berhasil di-insert
        """
        conn = None
        try:
            info("🚀 STARTING FACEBOOK INGESTION PROCESS")
            
            # Step 1: Fetch data dari Facebook API
            info(f"📥 Fetching {post_limit} latest posts from Facebook...")
            raw_data = fetch_facebook_data(limit=post_limit)
            
            if not raw_data:
                error("❌ No data available for ingestion")
                return 0
            
            # Step 2: Setup database connection
            conn = get_conn()
            if not conn:
                error("❌ Database connection failed")
                return 0
            
            # Step 3: Get or create Facebook channel
            channel_id = get_or_create_channel(
                conn, 
                name=self.channel_name,
                type_=self.channel_type,
                base_url=self.base_url
            )
            
            if not channel_id:
                error("❌ Failed to get or create Facebook channel")
                return 0
            
            info(f"📝 Using channel ID: {channel_id}")
            
            # Step 4: Transform data untuk database
            info("🔄 Transforming Facebook data for database...")
            final_data = self._transform_facebook_data(raw_data, channel_id)
            
            if not final_data:
                info("ℹ️ No valid comments found for ingestion")
                return 0
            
            info(f"📊 Transformed {len(final_data)} comments for insertion")
            
            # Step 5: Insert ke database
            info("💾 Inserting data into database...")
            inserted_count = insert_raw_feedback(conn, final_data)
            
            # Step 6: Update last ingested timestamp
            update_channel_last_ingested(conn, channel_id)
            
            info(f"✅ FACEBOOK INGESTION COMPLETED - {inserted_count} records inserted")
            return inserted_count
            
        except Exception as e:
            error(f"💥 CRITICAL ERROR during Facebook ingestion: {e}")
            import traceback
            error(f"Stack trace: {traceback.format_exc()}")
            return 0
            
        finally:
            if conn:
                conn.close()
                info("🔚 Database connection closed")


# Fungsi utama untuk compatibility
def ingest_facebook(post_limit=3):
    """
    Main function untuk Facebook ingestion (legacy compatibility)
    
    Args:
        post_limit (int): Jumlah postingan yang akan di-process
        
    Returns:
        int: Jumlah records yang berhasil di-insert
    """
    ingestor = FacebookIngestor()
    return ingestor.ingest(post_limit)

