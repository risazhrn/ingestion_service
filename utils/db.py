import pymysql
import json
from config.settings import *


def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True  # pastikan autocommit supaya UPDATE tersimpan
    )


# ============================================================
# GET OR CREATE CHANNEL
# ============================================================
def get_or_create_channel(conn, name, type_=None, base_url=None):
    """
    Buat channel baru jika belum ada.
    last_ingested_at otomatis di-set NOW() saat channel baru dibuat.
    """
    try:
        with conn.cursor() as cur:
            # Cek channel sudah ada
            cur.execute("SELECT id FROM channels WHERE name = %s", (name,))
            row = cur.fetchone()
            if row:
                return row["id"]

            # Insert channel baru dengan last_ingested_at = NOW()
            cur.execute("""
                INSERT INTO channels (name, type, base_url, last_ingested_at)
                VALUES (%s, %s, %s, NOW())
            """, (name, type_, base_url))

            # Ambil kembali ID channel
            cur.execute("SELECT id FROM channels WHERE name = %s", (name,))
            row = cur.fetchone()
            return row["id"] if row else None

    except Exception as e:
        print("\n❌ ERROR get_or_create_channel()")
        print("Message:", str(e))
        return None


# ============================================================
# UPDATE LAST INGESTED
# ============================================================
def update_channel_last_ingested(conn, channel_id):
    if not channel_id:
        print("❌ update_channel_last_ingested: channel_id kosong")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE channels
                SET last_ingested_at = NOW()
                WHERE id = %s
            """, (channel_id,))
            print(f"DEBUG: update_channel_last_ingested executed for id={channel_id}")
        return True
    except Exception as e:
        print("\n❌ ERROR update_channel_last_ingested()")
        print("Message:", str(e))
        return False


# ============================================================
# INSERT OR UPDATE RAW FEEDBACK
# ============================================================
def insert_raw_feedback(conn, items):
    if not items:
        print("⚠ insert_raw_feedback: empty items")
        return 0

    success = 0
    check_sql = "SELECT id FROM raw_feedback WHERE source_url = %s LIMIT 1"
    insert_sql = """
        INSERT INTO raw_feedback
        (channel_id, author_name, rating, content, source_url, review_created_at, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    update_sql = """
        UPDATE raw_feedback
        SET author_name=%s,
            rating=%s,
            content=%s,
            review_created_at=%s,
            metadata=%s
        WHERE source_url=%s
    """

    for idx, item in enumerate(items):
        try:
            source_url = item.get("source_url")
            content = item.get("content")
            if not content:
                print(f"⚠ Skip item index {idx}: content kosong")
                continue

            with conn.cursor() as cur:
                cur.execute(check_sql, (source_url,))
                exist = cur.fetchone()
                if exist:
                    cur.execute(update_sql, (
                        item.get("author_name"),
                        item.get("rating"),
                        content,
                        item.get("review_created_at"),
                        json.dumps(item.get("metadata", {})),
                        source_url
                    ))
                else:
                    cur.execute(insert_sql, (
                        item.get("channel_id"),
                        item.get("author_name"),
                        item.get("rating"),
                        content,
                        source_url,
                        item.get("review_created_at"),
                        json.dumps(item.get("metadata", {}))
                    ))

            success += 1
        except Exception as e:
            print("\n❌ ERROR insert_raw_feedback()")
            print("Message:", str(e))
            print("Item index:", idx)
            print("Item data:", json.dumps(item, indent=2, default=str))
            continue

    print(f"\n✅ insert_raw_feedback selesai. Total sukses: {success}/{len(items)}")
    return success
