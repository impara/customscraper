from playwright.async_api import async_playwright


class PlaywrightSetup:
    def __init__(self, base_url):
        self.base_url = base_url

    async def setup(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        await self.page.goto(self.base_url)
        return self.page

    async def teardown(self):
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()
