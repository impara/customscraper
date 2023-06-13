import json
import asyncio
from datetime import datetime
from sqlalchemy import select
from urllib.parse import urljoin
from redis_cache import RedisCache
import logging
from model import ToolTable
from database import async_session
from utils import create_async_db_connection
from sqlalchemy.exc import StatementError, IntegrityError
from abc import ABC, abstractmethod
from playwright.async_api import async_playwright
from playwright_setup import PlaywrightSetup


from urllib.parse import urlparse
import time

# Create a custom logger
logger = logging.getLogger(__name__)
# Set your constants
IGNORE_LAST_SCRAPED = False


async def startup_event_scrape(scrape_limit, scraper):
    # Initialize RedisCache
    redis_cache = RedisCache()
    redis_pool = await redis_cache.get_redis_pool()

    # Create an instance of PlaywrightSetup
    # Use the base_url from the scraper
    playwright_setup = PlaywrightSetup(scraper.base_url)
    # Start the Playwright process and open a new page
    page = await playwright_setup.setup()

    try:
        async with async_session() as session:
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


class BaseScraper(ABC):
    def __init__(self, base_url, scrape_limit, redis_cache, session):
        self.base_url = base_url
        self.scrape_limit = scrape_limit
        self.redis_cache = redis_cache
        self.session = session
        self.playwright = None
        self.browser = None
        self.page = None

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
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch()
        print(f"Returning driver: {self.browser}")
        self.page = await self.browser.new_page()

    async def teardown(self):
        await self.browser.close()
        await self.playwright.stop()

    async def safe_get_attribute(self, element, attr_name, retries=1):
        for _ in range(retries + 1):
            try:
                return await element.get_attribute(attr_name)
            except Exception:
                continue
        return None

    async def extract_tool_data(self, tool_url):
        # Navigate to the tool URL
        await self.page.goto(tool_url)

        # Add a delay to respect rate limits
        await asyncio.sleep(0.5)

        try:
            # Extract the tool information
            name = await self.page.inner_text("h1.heading-3")
            pricing_model = await self.page.inner_text("div.text-block-2")
            description = await self.page.inner_text("div.rich-text-block.w-richtext")
            additional_info = await self.page.inner_text("div.text-block-18")
            # Extract redirected tool elements
            final_url = await self.page.get_attribute("div.div-block-6 > a", "href")

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
            print(
                f"Unexpected error extracting data from {tool_url}: {str(e)}")
            return None

    async def fetch(self, url, max_retries=2):
        # Defining the retry loop
        for attempt in range(max_retries):
            try:
                # Open a new tab
                new_page = await self.browser.new_page()
                # Navigate to the URL
                await new_page.goto(url)
                # Get the final URL
                final_url = new_page.url
                # Close the current tab
                await new_page.close()
                return final_url
            except Exception as e:
                print(f'Error occurred when fetching URL {url}: {e}')
                # If it's not the last attempt, wait for a bit before retrying
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    return None  # Return None after all retries have failed

    async def extract_links(self):
        try:
            # Wait for the page to load
            await self.page.wait_for_load_state("networkidle")
        except Exception:
            print(
                "TimeoutException occurred while waiting for elements. Skipping this page.")
            return []

        await asyncio.sleep(0.5)
        # Extract site tool elements
        tool_elements = await self.page.query_selector_all("div.tool-item-text-link-block---new a.tool-item-link---new")

        # Log the number of links found
        print(f"Number of tool links found: {len(tool_elements)}")

        # Fetch all tool URLs at once
        tool_urls = []
        for element in tool_elements:
            href = await self.safe_get_attribute(element, "href")
            tool_urls.append(urljoin(self.base_url, href))

        return tool_urls

    async def scrape_tools(self):
        if self.page is not None:
            await self.page.goto(self.base_url)

            tools_scraped = 0
            start_scraping = False

            while tools_scraped < self.scrape_limit:
                while not start_scraping:
                    tool_urls = await self.extract_links()

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
                    tool_data = await self.extract_tool_data(tool_url)
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
                    fetch_url = await self.fetch(tool_data['final_url'])

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
                    scroll_height = await self.page.evaluate("() => document.body.scrollHeight")
                    for i in range(0, scroll_height, 250):
                        await self.page.evaluate(f"window.scrollTo(0, {i})")
                        try:
                            await self.page.wait_for_selector("div.tool-item-text-link-block---new a.tool-item-link---new", timeout=10000)
                        except Exception:
                            print(
                                "TimeoutException occurred while waiting for elements. Skipping this page.")
                        # Wait for the new tools to load
                        await asyncio.sleep(0.5)

                else:
                    print("No new tools found after scrolling. Stopping the scraper.")
                    break

            # Close the session after all tools have been processed
            self.session.expunge_all()
            await self.session.close()
            await self.browser.close()
        else:
            raise RuntimeError("Failed to initialize driver")
