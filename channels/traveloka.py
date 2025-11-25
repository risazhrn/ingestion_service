from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import time
import re
from datetime import datetime, timedelta


def crawl_traveloka_reviews(hotel_url, max_pages=5):
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-features=VizDisplayCompositor")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)
    
    reviews_data = []
    collected_reviews = set()

    try:
        print(f"Navigating to: {hotel_url}")
        driver.get(hotel_url)
        time.sleep(8)
        
        wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
        
        hotel_name = "Unknown"
        try:
            name_tag = driver.find_element(By.CSS_SELECTOR, 'h1')
            if name_tag:
                hotel_name = name_tag.text.strip()
                print(f"Hotel: {hotel_name}")
        except Exception as e:
            print(f"Could not find hotel name: {e}")

        review_tab = find_review_tab(driver, wait)
        if not review_tab:
            print("Failed to find review tab, returning empty data")
            return hotel_name, []
        
        driver.execute_script("arguments[0].click();", review_tab)
        print("Clicked on reviews tab")
        time.sleep(5)
        
        try:
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.css-1dbjc4n.r-14lw9ot.r-h1746q.r-kdyh1x.r-d045u9.r-1udh08x.r-d23pfw'))
            )
            print("Review container loaded successfully")
        except TimeoutException:
            print("Review container not found, attempting to continue")

        current_page = 0
        while current_page < max_pages:
            print(f"Processing Page {current_page + 1}")
            
            scroll_and_load_reviews(driver, wait)
            time.sleep(2)
            
            review_tags = driver.find_elements(By.CSS_SELECTOR, 'div.css-1dbjc4n.r-14lw9ot.r-h1746q.r-kdyh1x.r-d045u9.r-1udh08x.r-d23pfw')
            
            if not review_tags:
                print("No reviews found on this page")
                break
                
            print(f"Found {len(review_tags)} reviews to process")
            
            page_reviews_count = process_reviews(review_tags, collected_reviews, reviews_data, driver)
            
            print(f"Added {page_reviews_count} new reviews from page {current_page + 1}")
            print(f"Total unique reviews collected: {len(reviews_data)}")
            
            if not click_next_page(driver, wait):
                print("No more pages available")
                break
            
            current_page += 1
            
            if len(reviews_data) >= 100:
                print("Reached target of 100 reviews")
                break

        print(f"Scraping completed. Total reviews collected: {len(reviews_data)}")
        return hotel_name, reviews_data

    except Exception as e:
        print(f"Error during crawling: {str(e)}")
        return hotel_name, reviews_data

    finally:
        driver.quit()
        print("Browser closed")


def find_review_tab(driver, wait):
    tab_selectors = [
        'div[data-testid="tabItem-reviews"]',
        'div[data-testid*="review"]',
        'div[role="tab"][aria-label*="Review"]',
        '//div[contains(text(), "Review")]',
        '//div[@role="tab" and contains(text(), "Review")]'
    ]
    
    for selector in tab_selectors:
        try:
            if selector.startswith('//'):
                review_tab = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
            else:
                review_tab = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
            print(f"Found review tab: {selector}")
            return review_tab
        except:
            continue
    
    try:
        review_tab = driver.find_element(By.XPATH, '//div[contains(text(), "Review")]')
        print("Found review tab using text search")
        return review_tab
    except:
        print("Review tab not found with any selector")
        return None


def process_reviews(review_tags, collected_reviews, reviews_data, driver):
    page_reviews_count = 0
    
    for i, review in enumerate(review_tags):
        try:
            review_data = get_review_data(review)
            if review_data:
                review_key = f"{review_data['author_name']}_{review_data['content'][:100]}"
                
                if review_key not in collected_reviews:
                    collected_reviews.add(review_key)
                    reviews_data.append(review_data)
                    page_reviews_count += 1
        except StaleElementReferenceException:
            review_tags = refresh_review_elements(driver)
            if i < len(review_tags):
                try:
                    review_data = get_review_data(review_tags[i])
                    if review_data:
                        review_key = f"{review_data['author_name']}_{review_data['content'][:100]}"
                        if review_key not in collected_reviews:
                            collected_reviews.add(review_key)
                            reviews_data.append(review_data)
                            page_reviews_count += 1
                except:
                    continue
        except Exception as e:
            print(f"Error processing review {i}: {str(e)}")
    
    return page_reviews_count


def refresh_review_elements(driver):
    return driver.find_elements(By.CSS_SELECTOR, 'div.css-1dbjc4n.r-14lw9ot.r-h1746q.r-kdyh1x.r-d045u9.r-1udh08x.r-d23pfw')


def get_review_data(review_element):
    try:
        review_name_tag = review_element.find_element(By.CSS_SELECTOR, 'div.css-901oao.r-uh8wd5.r-ubezar.r-b88u0q.r-135wba7.r-fdjqy7')
        review_content_tag = review_element.find_element(By.CSS_SELECTOR, 'div.css-1dbjc4n.r-1udh08x > div.css-1dbjc4n > div.css-901oao.r-uh8wd5.r-1b43r93.r-majxgm.r-rjixqe.r-fdjqy7')
        rating_title_tag = review_element.find_element(By.CSS_SELECTOR, 'div[data-testid="tvat-ratingScore"]')
        review_date_tag = review_element.find_element(By.CSS_SELECTOR, 'div.css-901oao.r-1ud240a.r-uh8wd5.r-1b43r93.r-b88u0q.r-1cwl3u0.r-fdjqy7')
        
        author_name = review_name_tag.text.strip()
        content = review_content_tag.text.strip()
        rating_text = rating_title_tag.text.strip()
        review_date_text = review_date_tag.text.strip()
        
        # Extract numeric rating only
        rating = extract_numeric_rating(rating_text)
        review_date = parse_review_date(review_date_text)
        
        return {
            "author_name": author_name,
            "content": content,
            "rating": rating,
            "review_created_at": review_date,
            "metadata": {
                "source": "traveloka",
                "raw_date_text": review_date_text,
                "original_rating": rating_text
            }
        }
    except Exception as e:
        print(f"Error extracting review data: {str(e)}")
        return None


def extract_numeric_rating(rating_text):
    """Extract numeric rating from various formats"""
    try:
        # Remove non-numeric characters except comma and dot
        cleaned = re.sub(r'[^\d,.]', '', rating_text)
        
        # Replace comma with dot for decimal numbers
        cleaned = cleaned.replace(',', '.')
        
        # Convert to float
        rating = float(cleaned)
        
        # If rating is more than 10, divide by 10 (e.g., 97 -> 9.7)
        if rating > 10:
            rating = rating / 10
        
        return rating
    except:
        return None


def parse_review_date(date_text):
    try:
        parts = re.sub(r'\b(?:Reviewed|ago)\b|\(s\)', '', date_text).strip().split(' ')
        if (len(parts) != 2) or (not parts[0].isdigit()) or (parts[1] not in ['day', 'days', 'week', 'weeks', 'month', 'months']):
            return None
        
        if parts[1] in ['day', 'days']:
            delta_args = {'days': int(parts[0])}
        elif parts[1] in ['week', 'weeks']:
            delta_args = {'weeks': int(parts[0])}
        else:
            delta_args = {'days': int(parts[0]) * 30}
        
        return (datetime.today() - timedelta(**delta_args)).date()
    except:
        return None


def scroll_and_load_reviews(driver, wait):
    last_height = driver.execute_script("return document.body.scrollHeight")
    reviews_count = 0
    scroll_attempts = 0
    max_scroll_attempts = 5
    
    print("Scrolling to load reviews")
    
    while scroll_attempts < max_scroll_attempts:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        current_reviews = driver.find_elements(By.CSS_SELECTOR, 'div.css-1dbjc4n.r-14lw9ot.r-h1746q.r-kdyh1x.r-d045u9.r-1udh08x.r-d23pfw')
        
        if len(current_reviews) > reviews_count:
            print(f"Loaded {len(current_reviews)} reviews")
            reviews_count = len(current_reviews)
            scroll_attempts = 0
        else:
            scroll_attempts += 1
            print(f"No new reviews ({scroll_attempts}/{max_scroll_attempts})")
        
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        
        last_height = new_height
        time.sleep(1)
    
    print(f"Scrolling completed. Found {reviews_count} reviews")


def click_next_page(driver, wait):
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        next_selectors = [
            'div[data-testid="next-page-btn"]',
            'div[aria-live="polite"][role="button"]',
            '.css-18t94o4.css-1dbjc4n.r-1ihkh82.r-sdzlij',
            'div[role="button"][tabindex="0"] svg[data-id="IcSystemChevronRight"]'
        ]
        
        for selector in next_selectors:
            try:
                next_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                print(f"Found next button: {selector}")
                
                if "disabled" in next_button.get_attribute("class") or next_button.get_attribute("aria-disabled") == "true":
                    print("Next button is disabled")
                    return False
                
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(1)
                
                driver.execute_script("arguments[0].click();", next_button)
                print("Clicked next page")
                
                time.sleep(3)
                wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
                
                wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.css-1dbjc4n.r-14lw9ot.r-h1746q.r-kdyh1x.r-d045u9.r-1udh08x.r-d23pfw'))
                )
                
                return True
            except:
                continue
        
        print("Next button not found")
        return False
            
    except TimeoutException:
        print("Timeout waiting for next button")
        return False
    except Exception as e:
        print(f"Error clicking next button: {str(e)}")
        return False