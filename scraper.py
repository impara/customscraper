import json
import asyncio
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
from sqlalchemy import select
from sqlalchemy.exc import StatementError, IntegrityError
from redis_cache import RedisCache
from playwright.async_api import async_playwright
from playwright_setup import PlaywrightSetup
from model import ToolTable
from database import async_session
from utils import create_async_db_connection
from model import ToolTable

logging.basicConfig(level=logging.INFO)

# Create a custom logger
logger = logging.getLogger(__name__)
# Set your constants
IGNORE_LAST_SCRAPED = False


async def startup_event_scrape(scrape_limit, scraper):
    # Initialize RedisCache
    redis_cache = RedisCache()  # No need to call get_redis_pool

    # Create an instance of PlaywrightSetup
    playwright_setup = PlaywrightSetup(scraper.base_url)
    # Start the Playwright process and open a new page
    page = await playwright_setup.setup()

    try:
        async with async_session() as session:  # Create a new session for each operation
            # Use SQLAlchemy Core statements with async session
            scraper.redis_cache = redis_cache
            scraper.session = session
            scraper.page = page  # Pass the page object to the scraper
            await scraper.scrape()

    except StatementError as e:
        logger.error("Error during startup event: %s", e)
        raise

    finally:
        # After you're done with your scraping logic, close the browser and stop the Playwright process
        await playwright_setup.teardown()


class CustomScraper(PlaywrightSetup):
    def __init__(self, base_url, scrape_limit, redis_cache, session, page, browser):
        super().__init__(base_url)
        self.scrape_limit = scrape_limit
        self.redis_cache = redis_cache
        self.session = session
        self.page = page
        self.browser = browser
        self.lock = asyncio.Lock()

    async def initialize(self):
        self.page = await self.setup()

    async def safe_get_attribute(self, element, attr_name, retries=1):
        for _ in range(retries + 1):
            try:
                return await self.page.evaluate(f'el => el.getAttribute("{attr_name}")', element)
            except Exception:
                continue
        return None

    async def safe_inner_text(self, selector, retries=1):
        for _ in range(retries + 1):
            try:
                return await self.page.inner_text(selector)
            except Exception:
                continue
        return None

    async def extract_tool_data(self, tool_url):
        # Add a delay before each navigation
        await asyncio.sleep(1)

        for _ in range(3):  # Try up to 3 times
            try:
                # Navigate to the tool URL
                # Wait for up to 60 seconds
                await self.page.goto(tool_url, timeout=60000)
                break  # If the navigation succeeds, break out of the loop
            except Exception as e:
                if 'net::ERR_ABORTED' in str(e):
                    logging.error(
                        f"Navigation aborted for {tool_url}, retrying...")
                    await asyncio.sleep(1)  # Wait for a bit before retrying
                else:
                    raise  # If it's a different error, re-raise it
        try:
            # Extract the tool information
            name = await self.safe_inner_text("h1.heading-3")
            pricing_model = await self.safe_inner_text("div.text-block-2")
            description = await self.safe_inner_text("div.rich-text-block.w-richtext")
            additional_info = await self.safe_inner_text("div.text-block-18")
            # Extract redirected tool elements
            final_url = await self.safe_get_attribute(await self.page.query_selector("div.div-block-6 > a"), "href")

            # Return tool data as a dictionary
            return {
                'name': name.strip(),
                'pricing_model': pricing_model.strip(),
                'description': description.strip(),
                'additional_info': additional_info.strip(),
                'final_url': final_url,
                'url': tool_url,
            }
        except Exception as e:
            logging.error(
                f"Unexpected error extracting data from {tool_url}: {str(e)}")
            return None

    async def fetch(self, url, max_retries=2):
        for attempt in range(max_retries):
            try:
                new_page = await self.browser.new_page()
                await new_page.goto(url)
                await new_page.wait_for_load_state("networkidle")
                final_url = new_page.url
                await new_page.close()  # Close the page after fetching the URL
                return final_url
            except Exception as e:
                logging.error(f'Error occurred when fetching URL {url}: {e}')
                if attempt < max_retries - 1:
                    await asyncio.sleep(3 ** attempt)  # Exponential backoff
                    continue
                else:
                    return None  # Return None after all retries have failed

    async def get_tool_data_from_redis(self, tool_urls):
        # Create a dictionary with the tool URLs as keys and a boolean value indicating whether the URL exists in Redis
        tool_data_dict = {
            tool_url: await self.redis_cache.exists(tool_url) for tool_url in tool_urls}

        return tool_data_dict

    async def extract_links(self):
        try:
            # Wait for the page to load
            await self.page.wait_for_load_state("networkidle")
        except Exception:
            logging.warning(
                "TimeoutException occurred while waiting for elements. Skipping this page.")
            return []

        tool_elements = await self.page.query_selector_all("div.tool-item-text-link-block---new a.tool-item-link---new")
        num_tool_elements = len(tool_elements)

        # Add this logging
        logging.info(f"Initial tool links found: {num_tool_elements}")

        # If there are no tool elements, start scrolling
        if not tool_elements:
            last_key = await self.redis_cache.get_last_key()
            last_key_found = False

            # Start observing for intersection changes before scrolling
            try:
                await self.page.evaluate("""
                    // Define a function to scroll the page when the last element becomes visible
                    function scroll_when_visible(target) {
                        const observer = new IntersectionObserver((entries, observer) => {
                            entries.forEach(entry => {
                                if (entry.isIntersecting) {
                                    window.scrollBy(0, window.innerHeight);
                                    observer.unobserve(target);
                                }
                            });
                        });
                        observer.observe(target);
                    }

                    // Get the last tool element
                    const tool_elements = document.querySelectorAll("div.tool-item-text-link-block---new a.tool-item-link---new");
                    const last_tool_element = tool_elements[tool_elements.length - 1];
                    scroll_when_visible(last_tool_element);
                """)
            except Exception as e:
                logging.error(f"Error during scrolling: {str(e)}")
                return []

            # Wait for new tool elements to appear
            while True:
                new_tool_elements = await self.page.query_selector_all("div.tool-item-text-link-block---new a.tool-item-link---new")
                if len(new_tool_elements) > num_tool_elements:
                    num_tool_elements = len(new_tool_elements)
                    # Check if the last key is in the new tool elements
                    for element in new_tool_elements:
                        href = await self.safe_get_attribute(element, "href")
                        tool_url = urljoin(self.base_url, href)
                        if tool_url == last_key:
                            last_key_found = True
                            break
                    if last_key_found:
                        break
                else:
                    await asyncio.sleep(1)  # Wait a bit before checking again

                # Log the number of new tool links found
                # Move the log inside the loop
                logging.info(
                    f"Number of new tool links found: {len(new_tool_elements)}")

        else:
            # Log if the initial elements were found
            logging.info("Tool elements found without scrolling.")

        # Fetch all href attributes at once
        hrefs = await asyncio.gather(*[self.safe_get_attribute(element, "href") for element in tool_elements])

        # Join the base URL with each href
        tool_urls = [urljoin(self.base_url, href) for href in hrefs]
        # Log the tool URLs
        for url in tool_urls:
            logging.info(f"Tool URL: {url}")

        return tool_urls[:self.scrape_limit]

    async def scrape_tools(self):
        try:
            await self.page.goto(self.base_url)
            tools_scraped = 0
            processed_urls = set()  # Keep track of processed URLs
            previous_urls = set()  # Keep track of previous URLs
            while tools_scraped < self.scrape_limit:
                # Log the current number of tools scraped
                logging.info(f"Current tools scraped: {tools_scraped}")
                tool_urls = await self.extract_links()
                # If the current URLs are the same as the previous URLs, break the loop
                if set(tool_urls) == previous_urls:
                    logging.info("Same URLs detected. Stopping scraping.")
                    break
                previous_urls = set(tool_urls)  # Update the previous URLs
                # Log the number of extracted tool urls
                logging.info(f"Extracted links count: {len(tool_urls)}")
                tool_data_dict = await self.get_tool_data_from_redis(tool_urls)
                tasks = []
                for tool_url in tool_urls:
                    if tool_url not in processed_urls and tools_scraped < self.scrape_limit:
                        if tool_data_dict.get(tool_url):
                            logging.info(
                                f"Tool data for {tool_url} already exists in the cache. Skipping.")
                            continue
                        # pass the session here
                        tasks.append(self.process_tool_url(
                            tool_url, tool_data_dict))

                        # Add the URL to the set of processed URLs
                        processed_urls.add(tool_url)
                        tools_scraped += 1
                        # Log processed urls
                        logging.info(
                            f"Tool url processed: {tool_url}, total processed: {len(processed_urls)}")
                await asyncio.gather(*tasks)
            await self.session.commit()  # commit once after all tasks are done
            logging.info("Changes committed successfully.")
        except Exception as e:
            logging.error(f"Error during scraping: {str(e)}")
        finally:
            await self.upsert_tool_data_to_redis()

    async def process_tool_url(self, tool_url, tool_data_dict):
        async with self.lock:  # Add this line
            if tool_data_dict.get(tool_url):
                logging.info(
                    f"Tool data for {tool_url} already exists in the cache. Skipping.")
                return
            tool_data = await self.extract_tool_data(tool_url)
            if tool_data is None:
                logging.warning(
                    f"Failed to extract data from {tool_url}. Skipping this tool.")
                return
            await self.process_tool_data(tool_data, tool_url)

    async def upsert_tool_data_to_redis(self):
        # Fetch all tool keys from Redis at once
        tool_keys = await self.redis_cache.get_keys("*")

        # Convert the list of tool keys into a set for faster lookup and decode the binary values to strings
        tool_keys_set = set(key.decode() for key in tool_keys)

        # Fetch only the tool data that is not already in Redis
        result = await self.session.execute(select(ToolTable).where(ToolTable.url.notin_(tool_keys_set)))
        tools = result.scalars().all()

        # Upsert the tool data to Redis
        for tool in tools:
            tool_data = {
                'name': tool.name,
                'pricing_model': tool.pricing_model,
                'description': tool.description,
                'additional_info': tool.additional_info,
                'final_url': tool.final_url,
                'url': tool.url,
            }
            await self.redis_cache.set_to_redis(tool.url, tool_data, None)

    async def process_tool_data(self, tool_data, tool_url):
        # Check the cache first
        cached_tool_data = await self.redis_cache.get_from_redis(tool_url)
        if cached_tool_data:
            logging.info(
                f"Tool data for {tool_url} already exists in the cache. Skipping.")
            return
        logging.info(f"Scraped tool: {tool_data['name']}")
        logging.info(f"Pricing Model: {tool_data['pricing_model']}")
        logging.info(f"Description: {tool_data['description']}")
        logging.info(f"Additional Info: {tool_data['additional_info']}")
        logging.info(f"Final url: {tool_data['final_url']}")

        fetch_url = await self.fetch(tool_data['final_url'])
        if not fetch_url:
            logging.warning("Could not fetch URL {} after multiple retries or URL is not valid. Skipping...".format(
                tool_data['final_url']))
            return

        parsed_url = urlparse(fetch_url)
        if parsed_url.scheme and parsed_url.netloc:
            clean_url = parsed_url.scheme + "://" + parsed_url.netloc
        else:
            logging.warning(
                "The fetched URL {} could not be parsed correctly.".format(fetch_url))
            return

        tool = ToolTable(
            name=tool_data['name'],
            description=tool_data['description'],
            pricing_model=tool_data['pricing_model'],
            additional_info=tool_data['additional_info'],
            final_url=clean_url,
            url=tool_url,
            last_scraped=datetime.now(),
        )

        self.session.add(tool)  # Use the session attribute of the class

        try:
            await self.session.commit()  # Use the session attribute of the class
            logging.info(
                f"Changes committed successfully. Added tool to session: {tool.name}.")
        except IntegrityError:
            logging.warning(
                f"Tool {tool.name} already exists in the database. Skipping.")
            await self.session.rollback()  # Roll back the session
        except Exception as e:
            logging.error("Error committing changes to the database:", str(e))
