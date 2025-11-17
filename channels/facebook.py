import requests
import os
from dotenv import load_dotenv

load_dotenv()

PAGE_ID = os.getenv("FB_PAGE_ID")
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")

# Helper untuk masking token saat debug
def mask_token(token):
    if not token:
        return "None"
    return token[:10] + "..." + token[-5:]


def fetch_latest_posts(limit=3):
    """
    Fetch N postingan terbaru dari Facebook Page
    """
    if not PAGE_ID or not ACCESS_TOKEN:
        print("❌ ERROR: PAGE_ID atau ACCESS_TOKEN tidak ditemukan di .env")
        print(f"PAGE_ID: {PAGE_ID}")
        print(f"TOKEN: {mask_token(ACCESS_TOKEN)}")
        return []

    url = f"https://graph.facebook.com/v24.0/{PAGE_ID}/posts"
    params = {
        "limit": limit,
        "access_token": ACCESS_TOKEN
    }

    print("\n===== DEBUG FETCH LATEST POSTS =====")
    print("URL:", url)
    print("PARAMS:", params)
    print("PAGE_ID:", PAGE_ID)
    print("TOKEN:", mask_token(ACCESS_TOKEN))

    response = requests.get(url, params=params)
    json_data = response.json()

    print("RAW RESPONSE:", json_data)
    print("====================================\n")

    return json_data.get("data", [])


def fetch_post_comments(post_id):
    """
    Fetch komentar untuk satu post
    """
    url = f"https://graph.facebook.com/v24.0/{post_id}/comments"
    params = {
        "access_token": ACCESS_TOKEN
    }

    print("\n===== DEBUG FETCH COMMENTS =====")
    print("POST ID:", post_id)
    print("URL:", url)
    print("TOKEN:", mask_token(ACCESS_TOKEN))

    response = requests.get(url, params=params)
    json_data = response.json()

    print("RAW COMMENT RESPONSE:", json_data)
    print("================================\n")

    return json_data.get("data", [])


def fetch_facebook_data(limit=3):
    """
    Fetch posts + comments dalam 1 struktur data nested
    """
    print("\n🚀 START FETCH FACEBOOK DATA")
    result = []

    posts = fetch_latest_posts(limit)
    if not posts:
        print("❌ DEBUG: posts list is EMPTY — kemungkinan PAGE_ID atau TOKEN salah")
        return []

    for post in posts:
        post_id = post.get("id")
        if not post_id:
            continue

        comments = fetch_post_comments(post_id)

        result.append({
            "post_id": post_id,
            "created_time": post.get("created_time"),
            "message": post.get("message", ""),
            "comments": comments
        })

    print("🎉 DONE FETCHING FACEBOOK DATA")
    print("FINAL RESULT:", result)
    return result
