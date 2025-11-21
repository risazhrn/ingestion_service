from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import re
from datetime import datetime, timedelta
from utils.logger import info, error, warn

def crawl_traveloka_reviews(hotel_url, max_pages=5):
    """
    Crawl reviews dari Traveloka - improved version dengan error handling yang lebih baik
    Returns: hotel_name, reviews_data
    """
    info(f"🚀 Starting Traveloka crawling for: {hotel_url}")
    
    # Setup driver - SAMA PERSIS seperti code original
    options = webdriver.ChromeOptions()  
    options.add_argument('--start-maximized')  
    options.add_argument('--no-sandbox')  
    options.add_argument('--disable-dev-shm-usage')  
    options.add_argument('--disable-blink-features=AutomationControlled')  
    options.add_experimental_option('excludeSwitches', ['enable-automation'])  
    options.add_experimental_option('useAutomationExtension', False)  
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=IsolateOrigins,site-per-process')
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=options)  
    driver.implicitly_wait(10)
    
    try:
        # Navigate to hotel page - SAMA seperti original
        driver.get(hotel_url)
        time.sleep(10)
        
        # Get hotel name - SAMA seperti original
        name_tag = driver.find_element(By.CSS_SELECTOR, 'h1')
        hotel_name = name_tag.text.strip() if name_tag else "Unknown Hotel"
        info(f"🏨 Hotel: {hotel_name}")

        reviews_data = []
        count = 0
        
        while count < max_pages:
            try:
                # Find reviews - SAMA selector seperti original
                review_tags = driver.find_elements(By.CSS_SELECTOR, 'div.css-1dbjc4n.r-14lw9ot.r-h1746q.r-kdyh1x.r-d045u9.r-1udh08x.r-d23pfw')
                info(f"📄 Found {len(review_tags)} reviews on page {count + 1}")
                
                for review in review_tags:
                    try:
                        # Extract data - SAMA seperti original dengan improvement error handling
                        review_name_tag = review.find_element(By.CSS_SELECTOR, 'div.css-901oao.r-uh8wd5.r-ubezar.r-b88u0q.r-135wba7.r-fdjqy7')
                        review_content_tag = review.find_element(By.CSS_SELECTOR, 'div.css-1dbjc4n.r-1udh08x > div.css-1dbjc4n > div.css-901oao.r-uh8wd5.r-1b43r93.r-majxgm.r-rjixqe.r-fdjqy7')
                        rating_title_tag = review.find_element(By.CSS_SELECTOR, 'div[data-testid="tvat-ratingScore"]')
                        review_date_tag = review.find_element(By.CSS_SELECTOR, 'div.css-901oao.r-1ud240a.r-uh8wd5.r-1b43r93.r-b88u0q.r-1cwl3u0.r-fdjqy7')
                        
                        if review_name_tag and review_content_tag and rating_title_tag and review_date_tag:
                            
                            # Date parsing - SAMA logic seperti original dengan improvement
                            raw_date_text = review_date_tag.text.strip()
                            parts = re.sub(r'\b(?:Reviewed|ago)\b|\(s\)', '', raw_date_text).strip().split(' ')
                            
                            # Filter hanya yang valid (day/week dengan angka)
                            if (len(parts) == 2) and (parts[0].isdigit()) and (parts[1] in ['day', 'days', 'week', 'weeks']):
                                # Normalize time unit
                                time_unit = 'day' if parts[1] in ['day', 'days'] else 'week'
                                delta_args = {'day': {'days': int(parts[0])}, 'week': {'weeks': int(parts[0])}}
                                review_date = (datetime.today() - timedelta(**delta_args.get(time_unit, {}))).date()
                                
                                # Format untuk return (bukan untuk JSON file)
                                reviews_data.append({
                                    "author_name": review_name_tag.text.strip(),
                                    "content": review_content_tag.text.strip(),
                                    "rating": f"{rating_title_tag.text.strip()}", 
                                    "review_created_at": review_date,
                                    "metadata": {
                                        "hotel_name": hotel_name,
                                        "source_type": "crawl",
                                        "raw_date_text": raw_date_text  # Simpan original text untuk debugging
                                    }
                                })
                            else:
                                warn(f"Skipping review with invalid date format: {raw_date_text}")
                                
                    except Exception as e:
                        warn(f"Error extracting individual review: {e}")
                        continue
                
                # Next page - SAMA seperti original dengan improvement error handling
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="next-page-btn"]:not([aria-disabled="true"])')
                    next_button.click()
                    time.sleep(5)
                    count += 1
                    info(f"➡️ Moving to page {count + 1}")
                except Exception as e:
                    info(f"❌ No more pages available or next button disabled: {e}")
                    break
                
            except Exception as e:
                error(f"Error processing page {count + 1}: {e}")
                break

        info(f"✅ Crawling completed: {len(reviews_data)} reviews found for {hotel_name}")
        return hotel_name, reviews_data

    except Exception as e:
        error(f"❌ Crawling failed: {e}")
        return "Unknown Hotel", []
        
    finally:
        driver.quit()
        info("🔚 Chrome driver closed")