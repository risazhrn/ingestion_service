# filename: channel/facebook.py
import requests
from dotenv import load_dotenv
from utils.logger import info, error
from config.settings import FB_BASE_URL, FB_PAGE_ID, FB_ACCESS_TOKEN

load_dotenv()

class FacebookAPI:
    def __init__(self):
        self.page_id = FB_PAGE_ID
        self.access_token = FB_ACCESS_TOKEN
        self.base_url = FB_BASE_URL or "https://graph.facebook.com/v24.0"

    def _mask_token(self, token):
        if not token:
            return "None"
        return token[:10] + "..." + token[-5:]

    def _validate_credentials(self):
        if not self.page_id or not self.access_token:
            error("❌ Facebook credentials missing: PAGE_ID atau ACCESS_TOKEN tidak ditemukan")
            info(f"PAGE_ID: {self.page_id}")
            info(f"TOKEN: {self._mask_token(self.access_token)}")
            return False
        return True

    def _make_api_request(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        default_params = {"access_token": self.access_token}
        if params:
            default_params.update(params)
        info(f"🌐 Facebook API Request: {endpoint} params={params}")
        try:
            response = requests.get(url, params=default_params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            error(f"❌ Facebook API Error: {e} (endpoint={endpoint})")
            return None

    def fetch_latest_posts(self, limit=3):
        """
        Fetch latest posts from the page with important fields.
        """
        if not self._validate_credentials():
            return []
        params = {
            "limit": limit,
            "fields": "id,message,created_time"
        }
        result = self._make_api_request(f"{self.page_id}/posts", params)
        return result.get("data", []) if result else []

    def fetch_post_comments(self, post_id, limit=100):
        """
        Fetch all comments for a post, handling pagination.
        Returns a flat list of comment objects (each is a dict).
        """
        if not self._validate_credentials():
            return []

        comments = []
        params = {
            "limit": limit,
            "fields": "id,message,from,created_time"
        }

        # initial request
        result = self._make_api_request(f"{post_id}/comments", params)
        if not result:
            return comments

        # collect data and follow paging.next
        try:
            while result:
                data = result.get("data", [])
                if data:
                    comments.extend(data)

                paging = result.get("paging", {})
                next_url = paging.get("next")
                if not next_url:
                    break

                # follow next page URL (it usually already includes access_token)
                try:
                    info(f"🌐 Fetching next page of comments: {next_url}")
                    r = requests.get(next_url, timeout=15)
                    r.raise_for_status()
                    result = r.json()
                except Exception as e:
                    error(f"❌ Error fetching next page: {e}")
                    break

        except Exception as e:
            error(f"❌ Error during comments pagination: {e}")

        return comments

    def fetch_facebook_data(self, limit=3):
        """
        Fetch posts + comments structured for ingestion.
        """
        info("🚀 Starting Facebook data fetch...")
        posts = self.fetch_latest_posts(limit)
        if not posts:
            error("❌ No posts fetched from Facebook")
            return []

        structured_data = []
        for post in posts:
            post_id = post.get("id")
            if not post_id:
                continue
            comments = self.fetch_post_comments(post_id)
            structured_data.append({
                "post_id": post_id,
                "created_time": post.get("created_time"),
                "message": post.get("message", ""),
                "comments": comments
            })

        info(f"✅ Facebook data fetch completed: {len(structured_data)} posts with comments")
        return structured_data


# singleton
facebook_api = FacebookAPI()

def fetch_facebook_data(limit=3):
    return facebook_api.fetch_facebook_data(limit)
