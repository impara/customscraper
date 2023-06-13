import json
import asyncio
from datetime import datetime
from sqlalchemy import select
from selenium.webdriver.common.by import By
from selenium_setup import SeleniumSetup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, TimeoutException, WebDriverException
from urllib.parse import urljoin
from redis_cache import RedisCache
import logging
from model import ToolTable
from database import async_session
from utils import create_async_db_connection
from sqlalchemy.exc import StatementError, IntegrityError
from abc import ABC, abstractmethod

from urllib.parse import urlparse
import time
# Set the logging level for Selenium Wire
logging.getLogger('seleniumwire').setLevel(logging.WARNING)
# Create a custom logger
logger = logging.getLogger(__name__)
# Set your constants
IGNORE_LAST_SCRAPED = False


async def startup_event_scrape(scrape_limit, scraper):
    # Initialize RedisCache
    redis_cache = RedisCache()
    redis_pool = await redis_cache.get_redis_pool()
    try:
        async with async_session() as session:
            # Use SQLAlchemy Core statements with async session
            scraper.redis_cache = redis_cache
            scraper.session = session
            await scraper.scrape()

    except StatementError as e:
        logger.error("Error during startup event: %s", e)
        raise


class BaseScraper(ABC):
    def __init__(self, base_url, scrape_limit, redis_cache, session):
        self.base_url = base_url
        self.scrape_limit = scrape_limit
        self.redis_cache = redis_cache
        self.session = session

    @abstractmethod
    async def setup(self):
        pass

    @abstractmethod
    async def scrape_tools(self):
        pass

    async def scrape(self):
        await self.setup()
        await self.scrape_tools()


class CustomScraper(BaseScraper):
    async def setup(self):
        selenium_setup = SeleniumSetup(self.base_url)
        self.driver = await selenium_setup.setup()
        print(f"Returning driver: {self.driver}")
        return self.driver

    def safe_get_attribute(self, element, attr_name, retries=1):
        for _ in range(retries + 1):
            try:
                return element.get_attribute(attr_name)
            except StaleElementReferenceException:
                continue
        return None

    def extract_tool_data(self, tool_url):
        # Navigate to the tool URL
        self.driver.get(tool_url)

        # Wait for the page to load
        WebDriverWait(self.driver, 10).until(
            lambda driver: driver.execute_script(
                "return document.readyState") == "complete"
        )

        # Wait for AJAX calls to complete
        # WebDriverWait(self.driver, 10).until(
        #    lambda driver: driver.execute_script("return jQuery.active") == 0
        # )

        # Add a delay to respect rate limits
        time.sleep(0.5)

        try:
            # Extract the tool information
            name = self.driver.find_element(
                By.CSS_SELECTOR, "h1.heading-3").text.strip()
            pricing_model = self.driver.find_element(
                By.CSS_SELECTOR, "div.text-block-2").text.strip()
            description = self.driver.find_element(
                By.CSS_SELECTOR, "div.rich-text-block.w-richtext").text.strip()
            additional_info = self.driver.find_element(
                By.CSS_SELECTOR, "div.text-block-18").text.strip()
            # Extract redirected tool elements
            redirected_tool_elements = self.driver.find_element(
                By.CSS_SELECTOR, "div.div-block-6 > a")

            # Get all redirected tool URLs
            final_url = redirected_tool_elements.get_attribute("href")

            # Return tool data as a dictionary
            return {
                'name': name,
                'pricing_model': pricing_model,
                'description': description,
                'additional_info': additional_info,
                'final_url': final_url,
                'url': tool_url,
            }
        except NoSuchElementException:
            print(f"Element not found when extracting data from {tool_url}")
            return None
        except Exception as e:
            print(
                f"Unexpected error extracting data from {tool_url}: {str(e)}")
            return None

    def fetch(self, url, max_retries=2):
        # Defining the retry loop
        for attempt in range(max_retries):
            try:
                # Open a new tab
                self.driver.execute_script("window.open('');")
                # Switch to the new tab (it's always the last one)
                self.driver.switch_to.window(self.driver.window_handles[-1])
                # Navigate to the URL
                self.driver.get(url)
                # Get the final URL
                final_url = self.driver.current_url
                # Close the current tab
                self.driver.close()
                # Switch back to the first tab
                self.driver.switch_to.window(self.driver.window_handles[0])
                return final_url
            except WebDriverException as e:
                logging.error(f'Error occurred when fetching URL {url}: {e}')
                # If it's not the last attempt, wait for a bit before retrying
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    return None  # Return None after all retries have failed

    def extract_links(self):
        try:
            # Wait for the page to load
            WebDriverWait(self.driver, 10).until(
                lambda driver: driver.execute_script(
                    "return document.readyState") == "complete"
            )
            # Wait for the tool links to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.tool-item-text-link-block---new a.tool-item-link---new"))
            )
        except TimeoutException:
            print(
                "TimeoutException occurred while waiting for elements. Skipping this page.")
            return []

        time.sleep(0.5)
        # Extract site tool elements
        tool_elements = self.driver.find_elements(
            By.CSS_SELECTOR, "div.tool-item-text-link-block---new a.tool-item-link---new")

        # Log the number of links found
        print(f"Number of tool links found: {len(tool_elements)}")

        # Fetch all tool URLs at once
        tool_urls = [urljoin(self.base_url, self.safe_get_attribute(element, "href"))
                     for element in tool_elements]

        return tool_urls

    async def scrape_tools(self):
        if self.driver is not None:
            self.driver.get(self.base_url)
            tools_scraped = 0
            start_scraping = False

            while tools_scraped < self.scrape_limit:
                while not start_scraping:
                    tool_urls = self.extract_links()

                    # Fetch all tool data from Redis at once
                    tool_data_list = await self.redis_cache.mget(*tool_urls)

                    # Convert the list of tool data into a dictionary with the tool URLs as keys
                    tool_data_dict = dict(zip(tool_urls, tool_data_list))

                    for tool_url in tool_urls:
                        if tools_scraped >= self.scrape_limit:
                            break
                        # Check if the tool data is in the local dictionary
                        tool_data = tool_data_dict.get(tool_url)
                        if tool_data is not None:
                            print(
                                f"Tools scraped so far: {tools_scraped}. Skipping: {tool_url} because it's already in Redis.")
                            continue

                        # Check if a tool with the same URL already exists in the database
                        existing_tool = await self.session.execute(
                            select(ToolTable).filter(
                                ToolTable.url == tool_url)
                        )
                        existing_tool = existing_tool.scalars().first()
                        if existing_tool:
                            # If the tool data is in the database, add it to Redis and skip this tool
                            print(
                                f"Tools scraped so far: {tools_scraped}. Adding: {tool_url} to Redis and skipping because it's already in the database.")
                            tool_data = {
                                'name': existing_tool.name,
                                'pricing_model': existing_tool.pricing_model,
                                'description': existing_tool.description,
                                'additional_info': existing_tool.additional_info,
                                'final_url': existing_tool.final_url,
                                'url': existing_tool.url,
                            }
                            await self.redis_cache.set_to_redis(
                                tool_url, tool_data, 86400)
                            continue
                        else:
                            start_scraping = True
                            break
                    # Tool is not in Redis cache or database, proceed with scraping
                    tool_data = self.extract_tool_data(tool_url)
                    if tool_data is None:
                        print(
                            f"Failed to extract data from {tool_url}. Skipping this tool.")
                        continue

                    # Log the tool information
                    print(f"Scraped tool {tools_scraped}: {tool_data['name']}")
                    print(f"Pricing Model: {tool_data['pricing_model']}")
                    print(f"Description: {tool_data['description']}")
                    print(f"Additional Info: {tool_data['additional_info']}")
                    print(f"Final url: {tool_data['final_url']}")

                    # Fetch the final URL
                    fetch_url = self.fetch(tool_data['final_url'])

                    # If fetch failed, skip this iteration
                    if not fetch_url:
                        logging.warning("Could not fetch URL {} after multiple retries. Skipping...".format(
                            tool_data['final_url']))
                        continue

                    parsed_url = urlparse(fetch_url)

                    # Check if the parsed URL has a scheme and netloc before assigning to clean_url
                    if parsed_url.scheme and parsed_url.netloc:
                        clean_url = parsed_url.scheme + "://" + parsed_url.netloc
                    else:
                        logging.warning(
                            "The fetched URL {} could not be parsed correctly.".format(fetch_url))
                        continue  # or handle this scenario in another suitable way for your use case

                    # Add the tool to the database
                    tool = ToolTable(
                        name=tool_data['name'],
                        description=tool_data['description'],
                        pricing_model=tool_data['pricing_model'],
                        additional_info=tool_data['additional_info'],
                        final_url=clean_url,
                        url=tool_url,
                        last_scraped=datetime.now(),
                    )
                    self.session.add(tool)

                    # Store the tool data in Redis
                    await self.redis_cache.set_to_redis(
                        tool_url, tool_data, 86400)
                    # Increment the tools_scraped counter after each tool is processed
                    tools_scraped += 1

                    # Commit the changes to the database
                    try:
                        await self.session.commit()
                        print(
                            f"Changes committed successfully. Added tool to session: {tool.name}.")

                    except IntegrityError:
                        print(
                            f"Tool {tool.name} already exists in the database. Skipping.")
                        await self.session.rollback()  # Roll back the session
                        continue

                    except Exception as e:
                        print("Error committing changes to the database:", str(e))

                start_scraping = False

                if not start_scraping:
                    print("No new tools found. Scrolling to load more tools.")

                    # Scroll in smaller increments and wait for the specific element to load
                    scroll_height = self.driver.execute_script(
                        "return document.body.scrollHeight;")
                    for i in range(0, scroll_height, 250):
                        self.driver.execute_script(f"window.scrollTo(0, {i});")
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, "div.tool-item-text-link-block---new a.tool-item-link---new"))
                        )
                        # Wait for the new tools to load
                        await asyncio.sleep(0.5)

                else:
                    print("No new tools found after scrolling. Stopping the scraper.")
                    break

            # Close the session after all tools have been processed
            self.session.expunge_all()
            await self.session.close()
            self.driver.quit()
        else:
            raise RuntimeError("Failed to initialize driver")
