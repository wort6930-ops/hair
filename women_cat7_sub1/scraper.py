import logging
from typing import List, Dict, Optional
import time
import re
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BoutiqaatScraper:
    """Scraper for Boutiqaat website with infinite scroll support"""

    def __init__(self):
        self.base_url = "https://www.boutiqaat.com"
        self.session_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'ar,en;q=0.9',
        }

    def _clean_url(self, url: str) -> str:
        """Ensure URL has proper scheme and remove double slashes"""
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = urljoin(self.base_url, url)
        # Fix double slashes in path (but not in protocol)
        import re
        url = re.sub(r'(?<!:)//+', '/', url)
        return url

    def _extract_image_url(self, soup, product_elem) -> str:
        img = product_elem.select_one('img.img-fluid')
        if img:
            image_url = img.get('data-src') or img.get('src')
            if image_url:
                return self._clean_url(image_url)
        return ''

    def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        import requests
        try:
            response = requests.get(url, headers=self.session_headers, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            return None

    def _make_request_with_js(self, url: str) -> Optional[BeautifulSoup]:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                logger.info(f"Loading page: {url}")
                page.goto(url, wait_until='domcontentloaded', timeout=90000)
                
                # Only apply infinite scroll for listing pages (/l/), not product detail pages (/p/)
                if '/l/' in url:
                    try:
                        page.wait_for_selector('div.single-product-wrap', timeout=30000)
                    except Exception:
                        logger.warning(f"No products found on page (selector timeout): {url}")
                    time.sleep(6)
                    logger.info("Starting infinite scroll...")
                    no_change_count = 0
                    max_attempts = 50
                    attempt = 0
                    while attempt < max_attempts:
                        current_count = page.evaluate("document.querySelectorAll('div.single-product-wrap').length")
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        time.sleep(6)
                        try:
                            page.wait_for_load_state('networkidle', timeout=8000)
                        except:
                            time.sleep(3)
                        new_count = page.evaluate("document.querySelectorAll('div.single-product-wrap').length")
                        logger.info(f"Scroll attempt {attempt + 1}: {current_count} -> {new_count} products")
                        if new_count == current_count:
                            no_change_count += 1
                            if no_change_count >= 5:
                                logger.info(f"No new products after 5 attempts. Stopping at {new_count} products.")
                                break
                        else:
                            no_change_count = 0
                        attempt += 1
                    final_count = page.evaluate("document.querySelectorAll('div.single-product-wrap').length")
                    logger.info(f"Scroll complete. Total products found: {final_count}")
                else:
                    # For product detail pages, just wait for content to load
                    logger.info("Product detail page - waiting for content...")
                    try:
                        page.wait_for_load_state('networkidle', timeout=10000)
                    except:
                        time.sleep(3)
                
                html_content = page.content()
                browser.close()
                return BeautifulSoup(html_content, 'html.parser')
        except Exception as e:
            logger.error(f"Browser request failed for {url}: {str(e)}")
            return None

    def get_products(self, category_url: str) -> List[Dict]:
        logger.info(f"Fetching products from: {category_url}")
        soup = self._make_request_with_js(category_url)
        if not soup:
            logger.error("Failed to load category page")
            return []
        return self._extract_all_products(soup, category_url)

    def _extract_all_products(self, soup: BeautifulSoup, category_url: str) -> List[Dict]:
        products = []
        product_elements = soup.select('div.single-product-wrap')
        logger.info(f"Found {len(product_elements)} product elements")
        for elem in product_elements:
            try:
                product = self._extract_product_details(elem, category_url)
                if product:
                    products.append(product)
            except Exception as e:
                logger.warning(f"Error extracting product: {str(e)}")
                continue
        logger.info(f"Successfully extracted {len(products)} products")
        return products

    def _extract_product_details(self, elem, category_url: str) -> Optional[Dict]:
        try:
            product = {}
            link_elem = elem.find('a', href=lambda x: x and '/p/' in str(x))
            if not link_elem or not link_elem.get('href'):
                return None
            product['url'] = self._clean_url(link_elem['href'])
            product['product_url'] = product['url']
            name_elem = elem.find('span', class_='product-name-plp-h3')
            if name_elem:
                product['name'] = name_elem.get_text(strip=True)
            else:
                product['name'] = link_elem.get('title', '') or link_elem.get_text(strip=True) or 'N/A'
            brand_elem = elem.find('span', class_='brand-name')
            product['brand'] = brand_elem.get_text(strip=True) if brand_elem else 'N/A'
            price_elem = elem.find('span', class_='new-price')
            product['price'] = price_elem.get_text(strip=True) if price_elem else 'N/A'
            old_price_elem = elem.find('span', class_='old-price')
            if old_price_elem:
                product['old_price'] = old_price_elem.get_text(strip=True)
            else:
                product['old_price'] = product['price']
            discount_elem = elem.find('span', class_='discount-price')
            product['discount'] = discount_elem.get_text(strip=True) if discount_elem else 'N/A'
            img_elem = elem.find('img', class_='img-fluid')
            if img_elem:
                image_url = img_elem.get('src') or img_elem.get('data-src')
                if image_url:
                    product['image_url'] = self._clean_url(image_url)
                else:
                    product['image_url'] = ''
            else:
                product['image_url'] = ''
            sku_match = re.search(r'/p/(\d+)', product['url'])
            product['sku'] = sku_match.group(1) if sku_match else 'N/A'
            url_parts = category_url.rstrip('/').split('/')
            if len(url_parts) >= 6:
                product['subcategory'] = url_parts[-2]
            else:
                product['subcategory'] = 'general'
            product['description'] = 'N/A'
            product['rating'] = 'N/A'
            product['reviews'] = 'N/A'
            product['colors'] = 'N/A'
            return product
        except Exception as e:
            logger.warning(f"Error extracting product: {str(e)}")
            return None

    def get_product_full_details(self, product_url: str) -> Optional[Dict]:
        """Scrape full details from product page"""
        # Clean the URL first
        product_url = self._clean_url(product_url)
        logger.info(f"Fetching full details from {product_url}")
        soup = self._make_request_with_js(product_url)
        
        if not soup:
            return None
        
        try:
            details = {}
            
            # SKU
            sku_elem = soup.find('span', class_='attr-level-val')
            details['sku'] = sku_elem.get_text(strip=True) if sku_elem else 'N/A'
            
            # Description
            desc_elem = soup.find('div', class_='content-color')
            details['description'] = desc_elem.get_text(strip=True) if desc_elem else 'N/A'
            
            # Rating
            rating_elem = soup.find('span', class_='product-ratting')
            details['rating'] = 'N/A'
            if rating_elem:
                # Count filled stars
                filled_stars = len(rating_elem.find_all('span', style=lambda x: x and 'width: 100%' in x))
                details['rating'] = f"{filled_stars}/5"
            
            # Review count
            review_elem = soup.find('a', href=lambda x: x and 'review' in str(x).lower())
            details['reviews'] = 'N/A'
            if review_elem:
                details['reviews'] = review_elem.get_text(strip=True)
            
            # Colors (look for color swatches)
            color_elems = soup.select('ul.color-list li, div.color-option')
            if color_elems:
                colors = []
                for c in color_elems[:10]:  # Limit to 10 colors
                    color_name = c.get('title') or c.get('data-original-title') or c.get_text(strip=True)
                    if color_name:
                        colors.append(color_name)
                details['colors'] = ', '.join(colors) if colors else 'N/A'
            else:
                details['colors'] = 'N/A'
            
            # Get old_price and discount from detail page if available
            old_price_elem = soup.find('span', class_='old-price')
            if old_price_elem:
                details['old_price'] = old_price_elem.get_text(strip=True)
            
            discount_elem = soup.find('span', class_='discount-price')
            if discount_elem:
                details['discount'] = discount_elem.get_text(strip=True)
            
            return details
        except Exception as e:
            logger.warning(f"Error extracting full details: {str(e)}")
            return {}
