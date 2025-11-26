import pymysql
import json
from config.settings import *
from datetime import datetime

def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

def get_or_create_channel(conn, name, type_=None, base_url=GOOGLE_BASE_URL):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM channels WHERE name = %s", (name,))
            row = cur.fetchone()
            if row:
                return row["id"]

            cur.execute("""
                INSERT INTO channels (name, type, base_url, last_ingested_at)
                VALUES (%s, %s, %s, NOW())
            """, (name, type_, base_url))

            cur.execute("SELECT id FROM channels WHERE name = %s", (name,))
            row = cur.fetchone()
            return row["id"] if row else None
    except Exception as e:
        print("❌ ERROR get_or_create_channel:", e)
        return None

def update_channel_last_ingested(conn, channel_id):
    if not channel_id:
        print("❌ channel_id kosong")
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE channels
                SET last_ingested_at = NOW()
                WHERE id = %s
            """, (channel_id,))
        return True
    except Exception as e:
        print("❌ ERROR update_channel_last_ingested:", e)
        return False

def insert_raw_feedback(conn, items):
    """
    Insert atau update review ke raw_feedback.
    Logic:
    - Jika item punya external_id: cek berdasarkan (channel_id, external_id)
        -> jika ada: update fields (content, metadata, review_created_at, source_url, author_name, rating)
        -> jika tidak ada: insert baru
    - Jika item tidak punya external_id: fallback ke cek author+rating+content (legacy)
    """
    if not items:
        print("⚠ insert_raw_feedback: empty items")
        return 0

    success = 0
    updated = 0
    duplicate_count = 0

    insert_sql = """
        INSERT INTO raw_feedback
        (channel_id, external_id, author_name, rating, content, source_url, review_created_at, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    check_by_external_sql = """
        SELECT id FROM raw_feedback
        WHERE channel_id = %s AND external_id = %s
        LIMIT 1
    """
    update_sql = """
        UPDATE raw_feedback
        SET author_name=%s, rating=%s, content=%s, source_url=%s, review_created_at=%s, metadata=%s, review_updated_at=NOW()
        WHERE id=%s
    """
    # legacy check
    check_legacy_sql = """
        SELECT id FROM raw_feedback 
        WHERE author_name=%s AND rating=%s AND content=%s
        LIMIT 1
    """

    for idx, item in enumerate(items):
        content = item.get("content")
        if not content or not content.strip():
            print(f"⚠ Skip idx {idx}: content kosong")
            continue

        channel_id = item.get("channel_id")
        external_id = item.get("external_id")
        try:
            with conn.cursor() as cur:
                if external_id:
                    # check by external id
                    cur.execute(check_by_external_sql, (channel_id, external_id))
                    existing = cur.fetchone()
                    if existing:
                        # update existing record
                        cur.execute(update_sql, (
                            item.get("author_name"),
                            item.get("rating"),
                            content,
                            item.get("source_url"),
                            item.get("review_created_at"),
                            json.dumps(item.get("metadata", {})),
                            existing["id"]
                        ))
                        updated += 1
                    else:
                        # insert new
                        cur.execute(insert_sql, (
                            channel_id,
                            external_id,
                            item.get("author_name"),
                            item.get("rating"),
                            content,
                            item.get("source_url"),
                            item.get("review_created_at"),
                            json.dumps(item.get("metadata", {}))
                        ))
                        success += 1
                else:
                    # legacy path: check by author+rating+content
                    cur.execute(check_legacy_sql, (item.get("author_name"), item.get("rating"), content))
                    exist = cur.fetchone()
                    if exist:
                        duplicate_count += 1
                        continue
                    cur.execute(insert_sql, (
                        channel_id,
                        None,
                        item.get("author_name"),
                        item.get("rating"),
                        content,
                        item.get("source_url"),
                        item.get("review_created_at"),
                        json.dumps(item.get("metadata", {}))
                    ))
                    success += 1

        except Exception as e:
            print(f"❌ ERROR insert_raw_feedback idx={idx}: {e}")
            continue

    # Summary output
    total_written = success + updated
    if total_written == 0:
        if duplicate_count > 0:
            print(f"\n📊 Tidak ada pembaruan data. Semua data sudah ada di database. (duplicate_count={duplicate_count})")
        else:
            print(f"\n📊 Tidak ada data yang berhasil diproses.")
    else:
        print(f"\n✅ Data terbaru berhasil ditambahkan/diupdate. Inserted: {success}, Updated: {updated}")
        if duplicate_count > 0:
            print(f"📋 Skip (legacy duplicates): {duplicate_count}")

    return total_written