import requests
from datetime import datetime
from utils.logger import info, error
from config.settings import GOOGLE_API_KEY, GOOGLE_PLACE_ID, GOOGLE_BASE_URL

def translate_to_indonesia(text):
    """
    Auto translate text ke Bahasa Indonesia
    """
    try:
        from googletrans import Translator
        translator = Translator()
        
        # Detect language dulu
        detection = translator.detect(text)
        source_lang = detection.lang
        
        # Jika bukan Indonesia, lakukan translate
        if source_lang != 'id':
            translation = translator.translate(text, src=source_lang, dest='id')
            info(f"🌐 Auto-translate: {source_lang} → id")
            return translation.text
        else:
            return text  # Sudah Indonesia, tidak perlu translate
            
    except Exception as e:
        error(f"Translation error: {e}")
        return text  # Return original jika error

def fetch_google_reviews():
    """
    Fetch data reviews dari Google Places API
    Returns: place_data, reviews_list
    """
    params = {
        'place_id': GOOGLE_PLACE_ID,
        'fields': 'name,rating,reviews,user_ratings_total,formatted_address',
        'key': GOOGLE_API_KEY,
        'language': 'id'
    }
    
    info("📡 Fetching data from Google Places API...")
    response = requests.get(GOOGLE_BASE_URL, params=params)
    data = response.json()
    
    if data.get("status") != "OK":
        error(f"Google API Error: {data}")
        return None, []
        
    place = data.get("result", {})
    reviews = place.get("reviews", [])
    
    info(f"✅ Successfully fetched {len(reviews)} reviews from Google Places API")
    return place, reviews

def process_google_reviews(reviews_data):
    """
    Process reviews data dengan auto-translate ke Bahasa Indonesia
    Returns: processed_reviews
    """
    processed_reviews = []
    
    for idx, review in enumerate(reviews_data):
        author = review.get("author_name") or ""
        content = review.get("text") or ""
        timestamp = review.get("time") or datetime.now().timestamp()
        review_time = datetime.fromtimestamp(timestamp)

        if not content.strip():
            info(f"⚠ Skip review {idx}: empty content")
            continue

        # AUTO-TRANSLATE KE BAHASA INDONESIA
        translated_content = translate_to_indonesia(content)
        
        # Log perbedaan jika ada translate
        if translated_content != content:
            info(f"   🔄 Review {idx+1}: Auto-translated to Indonesian")
            info(f"      Before: {content[:60]}...")
            info(f"      After:  {translated_content[:60]}...")

        processed_reviews.append({
            "author_name": author,
            "rating": review.get("rating"),
            "content": translated_content,  # Content yang sudah di-translate
            "source_url": review.get("author_url"),
            "review_created_at": review_time,
            "metadata": {
                "language": "id",  # Selalu set sebagai Indonesian
                "original_language": review.get("language", "en"),
                "original_content": content,  # Simpan content asli
                "profile_photo_url": review.get("profile_photo_url"),
                "relative_time": review.get("relative_time_description"),
                "translated": translated_content != content,  # True jika ada translate
                "auto_translated": True  # Flag auto-translate
            }
        })
    
    info(f"🔄 Processed {len(processed_reviews)} reviews with auto-translation")
    return processed_reviews