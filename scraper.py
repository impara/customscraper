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
    finally:
        # After you're done with your scraping logic, close the browser and stop the Playwright process
        await playwright_setup.teardown()
        await redis_cache.close()  # Close the Redis connection when done


class CustomScraper(PlaywrightSetup):
    def __init__(self, base_url, scrape_limit, redis_cache, session, page, browser):
        super().__init__(base_url)
        self.scrape_limit = scrape_limit
        self.redis_cache = redis_cache
        self.session = session
        self.page = page
        self.browser = browser
        self.lock = asyncio.Lock()
        self.all_urls = set()

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
                await self.page.goto(tool_url, wait_until="domcontentloaded", timeout=120000)
                break  # If the navigation succeeds, break out of the loop
            except Exception as e:
                if 'net::ERR_ABORTED' in str(e):
                    logger.error(
                        f"Navigation aborted for {tool_url}, retrying...")
                else:
                    raise  # If it's a different error, re-raise it
        try:
            # Extract the tool information
            name = await self.safe_inner_text("h1.heading-3")
            pricing_model = await self.safe_inner_text("div.text-block-2")
            description = await self.safe_inner_text("div.rich-text-block.w-richtext")
            additional_info = await self.safe_inner_text("div.text-block-18")
            # Extract redirected tool elements
            final_url_element = await self.page.query_selector("div.div-block-6 > a")
            final_url = await self.safe_get_attribute(final_url_element, "href")

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
            logger.error(
                f"Unexpected error extracting data from {tool_url}: {str(e)}")
            return None

    async def fetch(self, url):
        try:
            new_page = await self.browser.new_page()
            await new_page.goto(url, wait_until="load", timeout=60000)
            await new_page.wait_for_function("document.readyState === 'complete'")
            final_url = new_page.url
            await new_page.close()  # Close the page after fetching the URL
            return final_url
        except Exception as e:
            logger.error(
                f'Error occurred when fetching URL {url} using initial method: {e}')

            # Try 'networkidle' method as a fallback
            try:
                new_page = await self.browser.new_page()
                await new_page.goto(url)
                await new_page.wait_for_load_state('networkidle')
                final_url = new_page.url
                await new_page.close()  # Close the page after fetching the URL
                return final_url
            except Exception as e:
                logger.error(
                    f'Error occurred when fetching URL {url} with networkidle method: {e}')
                return None  # Return None if both methods fail

    async def get_tool_data_from_redis(self, tool_urls):
        # Collect all the Redis cache keys
        cache_keys = [tool_url for tool_url in tool_urls]

        # Retrieve the tool data for all cache keys in a single call
        tool_data_list = await self.redis_cache.mget(*cache_keys)

        # Create a dictionary with the tool URLs as keys and the existence status as values
        tool_data_dict = {tool_url: tool_data is not None for tool_url,
                          tool_data in zip(tool_urls, tool_data_list)}

        return tool_data_dict

    async def extract_links(self):
        tool_urls = []

        try:
            await self.page.wait_for_load_state("networkidle")
        except Exception:
            logger.warning(
                "TimeoutException occurred while waiting for elements. Skipping this page.")
            return tool_urls

        tool_elements = await self.page.query_selector_all("div.tool-item-text-link-block---new a.tool-item-link---new")
        num_tool_elements = len(tool_elements)
        logger.info(f"Initial tool links found: {num_tool_elements}")

        max_iterations = 100  # adjust this value as needed
        iterations = 0

        if num_tool_elements < self.scrape_limit:
            while True:
                if iterations >= max_iterations:
                    logger.info("Reached maximum number of iterations.")
                    break
                await self.page.evaluate('window.scrollTo(0, document.body.scrollHeight)')

                # Pause here to allow for new content to load
                await asyncio.sleep(2)  # 2 seconds pause

                new_tool_elements = await self.page.query_selector_all("div.tool-item-text-link-block---new a.tool-item-link---new")
                if len(new_tool_elements) > num_tool_elements:
                    num_tool_elements = len(new_tool_elements)
                else:
                    break

                logger.info(
                    f"Number of new tool links found: {len(new_tool_elements)}")
                current_urls = set([await element.get_attribute('href') for element in new_tool_elements])
                new_urls = current_urls.difference(self.all_urls)
                if not new_urls:
                    break
                else:
                    self.all_urls.update(new_urls)
                    logger.info(f"Number of new URLs found: {len(new_urls)}")

                if len(self.all_urls) >= self.scrape_limit:
                    break
                iterations += 1
        else:
            logger.info("Tool elements found without scrolling.")

        hrefs = await asyncio.gather(*[self.safe_get_attribute(element, "href") for element in tool_elements])
        tool_urls = [urljoin(self.base_url, href) for href in hrefs]
        self.all_urls.update(tool_urls)

        for url in tool_urls:
            logger.info(f"Tool URL: {url}")

        return tool_urls[:self.scrape_limit]

    async def scrape_tools(self):
        try:
            await self.page.goto(self.base_url)
            tools_scraped = 0
            while tools_scraped < self.scrape_limit:
                # Log the current number of tools scraped
                logger.info(f"Current tools scraped: {tools_scraped}")

                # save a copy of all_urls before extraction
                previous_all_urls = set(self.all_urls)
                tool_urls = await self.extract_links()

                # Exit condition if no new URLs are found after extraction
                if previous_all_urls == self.all_urls:
                    logger.info("No new URLs found. Exiting...")
                    break

                # Log the number of extracted tool URLs
                logger.info(f"Extracted links count: {len(tool_urls)}")
                tool_data_dict = await self.get_tool_data_from_redis(tool_urls)
                tasks = []
                for tool_url in tool_urls:
                    if not tool_data_dict.get(tool_url) and tools_scraped < self.scrape_limit:
                        tasks.append(self.process_tool_url(tool_url))
                        # Increase the count of tools_scraped regardless of the outcome of processing the URL
                        tools_scraped += 1
                        # Log processed URLs
                        logger.info(f"Total tools scraped: {tools_scraped}")

                result = await asyncio.gather(*tasks, return_exceptions=True)
                failed_urls = [url for url, res in zip(
                    tool_urls, result) if isinstance(res, Exception)]
                successful_scrapes = len(
                    [res for res in result if not isinstance(res, Exception)])
                tools_scraped += successful_scrapes
                logger.info(f"Total tools scraped: {tools_scraped}")

                # Retry failed URLs, if any
                if failed_urls:
                    logger.warning(f"Retrying failed URLs: {failed_urls}")
                    retry_tasks = [self.process_tool_url(
                        url) for url in failed_urls]
                    await asyncio.gather(*retry_tasks)

            logger.info("Changes committed successfully.")
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
        finally:
            await self.upsert_tool_data_to_redis()
            # Ensure that the Redis connection is closed after scraping is done
            if hasattr(self, 'redis_cache'):
                await self.redis_cache.close()
            logger.info("Finished scraping and closed Redis connection.")

    async def process_tool_url(self, tool_url):
        async with self.lock:
            try:
                tool_data = await self.extract_tool_data(tool_url)
                if tool_data is None:
                    logger.warning(
                        f"Failed to extract data from {tool_url}. Skipping this tool.")
                    self.all_urls.remove(tool_url)
                    return tool_url

                if tool_data['final_url'] is None:
                    logger.warning(
                        f"Failed to fetch final URL for {tool_url}. Skipping this tool.")
                    self.all_urls.remove(tool_url)
                    return

                await self.process_tool_data(tool_data, tool_url)
            except Exception as e:
                logger.error(f"Error during scraping {tool_url}: {str(e)}")
                self.all_urls.remove(tool_url)
                return None  # Explicitly return None for error cases

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
            logger.info(
                f"Tool data for {tool_url} already exists in the cache. Skipping.")
            return
        logger.info(f"Scraped tool: {tool_data['name']}")
        logger.info(f"Pricing Model: {tool_data['pricing_model']}")
        logger.info(f"Description: {tool_data['description']}")
        logger.info(f"Additional Info: {tool_data['additional_info']}")
        logger.info(f"Final url: {tool_data['final_url']}")

        fetch_url = await self.fetch(tool_data['final_url'].rstrip('/'))
        if not fetch_url:
            logger.warning("Could not fetch URL {} after multiple retries or URL is not valid. Skipping...".format(
                tool_data['final_url']))
            clean_url = tool_data['final_url']
        else:
            parsed_url = urlparse(fetch_url)
            if parsed_url.scheme and parsed_url.netloc:
                clean_url = parsed_url.scheme + "://" + parsed_url.netloc

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
            logger.info(
                f"Changes committed successfully. Added tool to session: {tool.name}.")
        except IntegrityError as e:
            logger.error(f"IntegrityError: {e}")
            await self.session.rollback()  # Roll back the session
        except Exception as e:
            logger.error("Error committing changes to the database:", str(e))
            await self.session.rollback()
