import pymysql
import json
from config.settings import *

def get_conn():
    """Buat koneksi ke database"""
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
    """Ambil channel, kalau tidak ada buat baru"""
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
    """Update kolom last_ingested_at"""
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
    Insert review ke raw_feedback.
    - Cek duplikat berdasarkan author_name + rating + content
    """
    if not items:
        print("⚠ insert_raw_feedback: empty items")
        return 0

    success = 0
    duplicate_count = 0
    insert_sql = """
        INSERT INTO raw_feedback
        (channel_id, author_name, rating, content, source_url, review_created_at, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    check_sql = """
        SELECT id FROM raw_feedback 
        WHERE author_name=%s AND rating=%s AND content=%s
        LIMIT 1
    """

    for idx, item in enumerate(items):
        content = item.get("content")
        if not content or not content.strip():
            print(f"⚠ Skip idx {idx}: content kosong")
            continue

        try:
            with conn.cursor() as cur:
                # Cek duplikat
                cur.execute(check_sql, (item.get("author_name"), item.get("rating"), content))
                exist = cur.fetchone()
                if exist:
                    duplicate_count += 1
                    continue

                # Insert baru
                cur.execute(insert_sql, (
                    item.get("channel_id"),
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

    # Print summary di akhir
    if success == 0:
        if duplicate_count > 0:
            print(f"\n📊 Tidak ada pembaruan data. Semua data sudah ada di database.")
        else:
            print(f"\n📊 Tidak ada data yang berhasil diproses.")
    else:
        print(f"\n✅ Data terbaru berhasil ditambahkan. Total: {success} data baru")
        if duplicate_count > 0:
            print(f"📋 Skip: {duplicate_count} data sudah ada")

    return success
