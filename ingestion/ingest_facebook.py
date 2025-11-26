# filename: ingestion/ingest_facebook.py
import hashlib
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

    def _parse_iso_datetime(self, date_string):
        """
        Robust ISO datetime parsing. Return datetime or None.
        We try multiple safe parsers; do NOT silently set to now
        (we will mark missing flag if parsing fails).
        """
        if not date_string:
            return None
        try:
            # Try fromisoformat (Python 3.7+)
            # Normalize Z -> +00:00 for fromisoformat
            ds = date_string
            if ds.endswith("Z"):
                ds = ds[:-1] + "+00:00"
            # Some APIs give "+0000" (no colon) — insert colon
            if len(ds) >= 5 and (ds[-5] in ['+', '-']) and (ds[-3] != ':'):
                # transform +0000 -> +00:00
                ds = ds[:-5] + ds[-5:-2] + ":" + ds[-2:]
            return datetime.fromisoformat(ds)
        except Exception:
            # Try a few common fallbacks
            formats = [
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(date_string, fmt)
                except Exception:
                    continue
        return None

    def _generate_external_id(self, author, content, created_at, prefix="fb"):
        """
        Deterministic fallback external_id generator when source doesn't provide one.
        """
        raw = f"{prefix}|{author}|{content}|{created_at}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def _transform_facebook_data(self, raw_data, channel_id):
        """
        Standardize Facebook data to ingestion format.
        Keeps rating as None (Facebook has no numeric rating).
        If created_time can't be parsed, falls back to ingestion time but marks metadata.
        """
        transformed_data = []
        for post in raw_data:
            post_id = post.get("post_id")
            post_message = post.get("message", "") or ""
            for comment in post.get("comments", []):
                # Pull basic fields safely
                author_name = (comment.get("from") or {}).get("name") or "Guest"
                content = comment.get("message") or ""
                raw_created = comment.get("created_time")

                parsed_date = self._parse_iso_datetime(raw_created)
                created_missing = False
                if parsed_date is None:
                    # DB requires a non-null review_created_at; fallback to ingestion time BUT mark metadata
                    parsed_date = datetime.utcnow()
                    created_missing = True

                comment_id = comment.get("id")
                if not comment_id or not str(comment_id).strip():
                    # generate deterministic external id fallback
                    comment_id = self._generate_external_id(author_name, content[:300], raw_created or parsed_date.isoformat())

                transformed_comment = {
                    "channel_id": channel_id,
                    "external_id": comment_id,
                    "author_name": author_name,
                    "rating": None,  # Facebook does not provide a numeric rating
                    "content": content,
                    "source_url": f"https://facebook.com/{post_id}" if post_id else None,
                    "review_created_at": parsed_date,
                    "metadata": {
                        "source": "facebook",
                        "comment_id": comment.get("id"),
                        "post_id": post_id,
                        "post_message": post_message[:200],
                        "original_data": comment,
                        "created_time_raw": raw_created,
                        "created_time_missing": created_missing
                    }
                }

                if transformed_comment["content"] and transformed_comment["content"].strip():
                    transformed_data.append(transformed_comment)
                else:
                    info(f"⚠️ Skipping empty comment from {transformed_comment['author_name']} (id={comment_id})")
        return transformed_data

    def ingest(self, post_limit=3):
        conn = None
        try:
            info("🚀 STARTING FACEBOOK INGESTION PROCESS")
            raw_data = fetch_facebook_data(limit=post_limit)
            if not raw_data:
                error("❌ No data available for ingestion")
                return 0

            conn = get_conn()
            if not conn:
                error("❌ Database connection failed")
                return 0

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
            info("🔄 Transforming Facebook data for database...")
            final_data = self._transform_facebook_data(raw_data, channel_id)

            if not final_data:
                info("ℹ️ No valid comments found for ingestion")
                return 0

            info(f"📊 Transformed {len(final_data)} comments for insertion")
            info("💾 Inserting data into database...")
            inserted_count = insert_raw_feedback(conn, final_data)

            # Update channel timestamp even if inserted_count == 0 (we polled)
            update_channel_last_ingested(conn, channel_id)

            info(f"✅ FACEBOOK INGESTION COMPLETED - {inserted_count} records inserted/updated")
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


def ingest_facebook(post_limit=3):
    ingestor = FacebookIngestor()
    return ingestor.ingest(post_limit)
