import requests
from datetime import datetime
from utils.logger import info, error
from config.settings import GOOGLE_API_KEY, GOOGLE_PLACE_ID, GOOGLE_BASE_URL

# ---------------------------------------------------------------------
# TRANSLATION
# ---------------------------------------------------------------------
def translate_to_indonesia(text):
    """
    Auto-translate review ke Bahasa Indonesia jika bukan bahasa Indonesia.
    Digunakan googletrans, tapi aman fallback jika error.
    """
    try:
        from googletrans import Translator
        translator = Translator()

        detection = translator.detect(text)
        source_lang = detection.lang

        if source_lang != "id":
            translated = translator.translate(text, src=source_lang, dest="id")
            info(f"🌐 Auto-translate: {source_lang} → id")
            return translated.text

        return text

    except Exception as e:
        error(f"[Translate Error] {e}")
        return text  # fallback: return original


# ---------------------------------------------------------------------
# GOOGLE API FETCH
# ---------------------------------------------------------------------
def fetch_google_reviews():
    """
    Fetch data review dari Google Places API.
    Return: place_info, reviews_list
    """
    params = {
        "place_id": GOOGLE_PLACE_ID,
        "fields": "name,rating,reviews,user_ratings_total,formatted_address",
        "key": GOOGLE_API_KEY,
        "language": "id",
    }

    info("📡 Fetching data from Google Places API...")
    try:
        response = requests.get(GOOGLE_BASE_URL, params=params, timeout=15)
        data = response.json()
    except Exception as e:
        error(f"❌ Request Failed: {e}")
        return None, []

    if data.get("status") != "OK":
        error(f"❌ Google API Error: {data}")
        return None, []

    place = data.get("result", {})
    reviews = place.get("reviews", [])

    info(f"✅ Successfully fetched {len(reviews)} reviews")
    return place, reviews


# ---------------------------------------------------------------------
# PROCESS REVIEWS
# ---------------------------------------------------------------------
def process_google_reviews(reviews_data):
    """
    Memproses review + auto-translate ke bahasa Indonesia.
    Return: processed_reviews (list)
    """
    processed = []

    for idx, review in enumerate(reviews_data):
        content = review.get("text") or ""
        if not content.strip():
            info(f"⚠ Skip review {idx}: empty content")
            continue

        author = review.get("author_name") or ""
        timestamp = review.get("time") or datetime.now().timestamp()
        review_time = datetime.fromtimestamp(timestamp)

        # ---- TRANSLATE ------------------------------------------------
        translated = translate_to_indonesia(content)

        if translated != content:
            info(f"🔄 Review {idx+1} auto-translated")
            info(f"   Before: {content[:60]}...")
            info(f"   After:  {translated[:60]}...")

        processed.append({
            "author_name": author,
            "rating": review.get("rating"),
            "content": translated,
            "source_url": review.get("author_url"),
            "review_created_at": review_time,
            "metadata": {
                "language": "id",
                "original_language": review.get("language", "unknown"),
                "original_content": content,
                "profile_photo_url": review.get("profile_photo_url"),
                "relative_time": review.get("relative_time_description"),
                "translated": translated != content,
                "auto_translated": True,
            }
        })

    info(f"🔍 Processed {len(processed)} reviews")
    return processed
