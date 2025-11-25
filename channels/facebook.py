import requests
import os
from dotenv import load_dotenv
from utils.logger import info, error
from config.settings import FB_BASE_URL, FB_PAGE_ID, FB_ACCESS_TOKEN

load_dotenv()


class FacebookAPI:
    def __init__(self):
        self.page_id = FB_PAGE_ID
        self.access_token = FB_ACCESS_TOKEN
        self.base_url = "https://graph.facebook.com/v24.0"
        
    def _mask_token(self, token):
        """Mask token untuk keamanan saat debug"""
        if not token:
            return "None"
        return token[:10] + "..." + token[-5:]
    
    def _validate_credentials(self):
        """Validasi kredensial Facebook"""
        if not self.page_id or not self.access_token:
            error("❌ Facebook credentials missing: PAGE_ID atau ACCESS_TOKEN tidak ditemukan")
            info(f"PAGE_ID: {self.page_id}")
            info(f"TOKEN: {self._mask_token(self.access_token)}")
            return False
        return True
    
    def _make_api_request(self, endpoint, params=None):
        """Helper untuk membuat request API Facebook"""
        url = f"{self.base_url}/{endpoint}"
        default_params = {"access_token": self.access_token}
        if params:
            default_params.update(params)
            
        info(f"🌐 Facebook API Request: {endpoint}")
        
        try:
            response = requests.get(url, params=default_params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            error(f"❌ Facebook API Error: {e}")
            return None
    
    def fetch_latest_posts(self, limit=3):
        """
        Fetch postingan terbaru dari Facebook Page
        
        Args:
            limit (int): Jumlah postingan yang akan di-fetch
            
        Returns:
            list: List postingan terbaru
        """
        if not self._validate_credentials():
            return []
            
        params = {"limit": limit}
        result = self._make_api_request(f"{self.page_id}/posts", params)
        
        return result.get("data", []) if result else []
    
    def fetch_post_comments(self, post_id):
        """
        Fetch semua komentar untuk sebuah post
        
        Args:
            post_id (str): ID post Facebook
            
        Returns:
            list: List komentar untuk post tersebut
        """
        if not self._validate_credentials():
            return []
            
        result = self._make_api_request(f"{post_id}/comments")
        return result.get("data", []) if result else []
    
    def fetch_facebook_data(self, limit=3):
        """
        Fetch posts beserta komentar-komentarnya
        
        Args:
            limit (int): Jumlah postingan yang akan di-fetch
            
        Returns:
            list: Structured data berisi posts dan komentar
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


# Singleton instance untuk mudah digunakan
facebook_api = FacebookAPI()

# Fungsi legacy untuk compatibility
def fetch_facebook_data(limit=3):
    return facebook_api.fetch_facebook_data(limit)