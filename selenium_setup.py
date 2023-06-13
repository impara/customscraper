from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
import asyncio


class SeleniumSetup:
    def __init__(self, base_url):
        self.base_url = base_url

    async def setup(self):
        options = ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        loop = asyncio.get_event_loop()
        self.driver = await loop.run_in_executor(None, lambda: webdriver.Chrome(options=options))
        await loop.run_in_executor(None, lambda: self.driver.get(self.base_url))
        return self.driver
