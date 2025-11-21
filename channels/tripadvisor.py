from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import datetime
from utils.logger import info, error, warn

def crawl_tripadvisor_reviews(hotel_url, max_pages=5):
    """
    Crawl reviews dari Tripadvisor
    Returns: hotel_name, reviews_data
    """
    info(f"🚀 Starting Tripadvisor crawling for: {hotel_url}")
    
    # Setup driver - langsung define options di sini
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
        # Navigate to hotel page
        driver.get(hotel_url)
        time.sleep(10)
        
        # Get hotel name
        name_tag = driver.find_element(By.CSS_SELECTOR, 'h1')
        hotel_name = name_tag.text.strip() if name_tag else "Unknown Hotel"
        info(f"🏨 Hotel: {hotel_name}")

        reviews_data = []
        count = 0
        
        while count < max_pages:
            try:
                # Find reviews
                review_tags = driver.find_elements(By.CSS_SELECTOR, 'div[data-test-target="HR_CC_CARD"]')
                info(f"📄 Found {len(review_tags)} reviews on page {count + 1}")
                
                for review in review_tags:
                    try:
                        # Extract data dari setiap review
                        review_name_tag = review.find_element(By.CSS_SELECTOR, 'span.biGQs._P.SewaP.OgHoE')
                        review_content_tag = review.find_element(By.CSS_SELECTOR, 'span.JguWG')
                        rating_title_tag = review.find_element(By.CSS_SELECTOR, 'svg[data-automation="bubbleRatingImage"] title')
                        review_date_tag = review.find_element(By.CSS_SELECTOR, 'div.biGQs._P.VImYz.AWdfh')
                        date_stay_tag = review.find_element(By.CSS_SELECTOR, 'span.biGQs._P.VImYz.xENVe')
                        
                        if all([review_name_tag, review_content_tag, rating_title_tag, review_date_tag, date_stay_tag]):
                            # Extract rating dari title attribute
                            rating_text = rating_title_tag.get_attribute('textContent').strip().split()[0]
                            
                            # Parse review date
                            raw_review_date = review_date_tag.text.strip().split(' wrote a review ')[-1]
                            
                            reviews_data.append({
                                "author_name": review_name_tag.text.strip(),
                                "content": review_content_tag.text.strip(),
                                "rating": f"{rating_text}/5",
                                "review_created_at": raw_review_date,
                                "metadata": {
                                    "hotel_name": hotel_name,
                                    "source_type": "crawl",
                                    "date_of_stay": date_stay_tag.text.strip(),
                                    "raw_review_date": raw_review_date
                                }
                            })
                            
                    except Exception as e:
                        warn(f"Error extracting individual Tripadvisor review: {e}")
                        continue
                
                # Next page
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, 'a[data-smoke-attr="pagination-next-arrow"]')
                    next_button.click()
                    time.sleep(5)
                    count += 1
                    info(f"➡️ Moving to Tripadvisor page {count + 1}")
                except Exception as e:
                    info(f"❌ No more pages available or next button not found: {e}")
                    break
                
            except Exception as e:
                error(f"Error processing Tripadvisor page {count + 1}: {e}")
                break

        info(f"✅ Tripadvisor crawling completed: {len(reviews_data)} reviews found for {hotel_name}")
        return hotel_name, reviews_data

    except Exception as e:
        error(f"❌ Tripadvisor crawling failed: {e}")
        return "Unknown Hotel"
        
    finally:
        driver.quit()
        info("🔚 Chrome driver closed for Tripadvisor")