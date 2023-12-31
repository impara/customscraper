from fastapi import FastAPI, Request, HTTPException, Query, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import List, Any
import logging
import os
from scraper import CustomScraper
from database import create_tables, get_db
from sqlalchemy import select, text, func
from sqlalchemy.exc import SQLAlchemyError
from model import ToolTable, ToolResponse
from sqlalchemy.ext.asyncio import AsyncSession
from redis_cache import RedisCache
from playwright_setup import PlaywrightSetup
import orjson


class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)


URL_TO_SCRAP = os.getenv("URL_TO_SCRAP")
logger = logging.getLogger(__name__)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

app = FastAPI(default_response_class=ORJSONResponse)


@app.get("/tools", response_model=List[ToolResponse])
async def get_tools(
    background_tasks: BackgroundTasks,
    skip: int = Query(
        0, description="Number of records to skip for pagination"),
    limit: int = Query(10, description="Maximum number of records to return"),
    additional_info: str = Query(
        None, description="Additional information to filter tools"),
    pricing_model: str = Query(
        None, description="Pricing model to filter tools"),
    search_term: str = Query(None, description="Search term to filter tools"),
    db: AsyncSession = Depends(get_db),
    redis_cache: RedisCache = Depends(RedisCache)
):
    """
    Retrieve a list of tools from the database.

    - **skip**: Number of records to skip for pagination
    - **limit**: Maximum number of records to return
    - **additional_info**: Additional information to filter tools
    - **pricing_model**: Pricing model to filter tools
    - **search_term**: Search term to filter tools
    """
    try:
        cache_key = f"tools:{skip}:{limit}:{additional_info}:{pricing_model}:{search_term}"
        tools_data = await redis_cache.get_from_redis(cache_key)

        if not tools_data:
            query = select(ToolTable).offset(skip).limit(limit)
            if additional_info:
                additional_info = additional_info.replace(
                    "\\", "\\\\").replace("%", "\\%")
                query = query.where(
                    ToolTable.additional_info.ilike(f"%{additional_info}%"))
            if pricing_model:
                pricing_model_lower = pricing_model.lower()
                query = query.where(func.lower(
                    ToolTable.pricing_model) == pricing_model_lower)
            if search_term:
                search_term = search_term.replace(
                    "\\", "\\\\").replace("%", "\\%")
                query = query.where(
                    text(
                        "(to_tsvector('english', name) @@ plainto_tsquery('english', :search)) OR "
                        "(to_tsvector('english', description) @@ plainto_tsquery('english', :search))"
                    ).params(search=search_term)
                )
            result = await db.execute(query)
            tools = result.scalars().all()
            tools_data = [tool.to_dict() for tool in tools]
            background_tasks.add_task(
                redis_cache.set_to_redis, cache_key, tools_data, expire=60*60*24)

        return tools_data
    except SQLAlchemyError as e:
        logging.error("Error fetching tools: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/additional_info", response_model=List[str])
async def get_additional_info(db: AsyncSession = Depends(get_db), redis_cache: RedisCache = Depends(RedisCache)):
    """
    Retrieve a list of distinct additional_info values from the database.
    """
    try:
        cache_key = "additional_info"
        additional_info_data = await redis_cache.get_from_redis(cache_key)

        if not additional_info_data:
            query = select(ToolTable.additional_info).distinct()
            result = await db.execute(query)
            additional_info_data = [row for row in result.scalars().all()]
            await redis_cache.set_to_redis(cache_key, additional_info_data, expire=60*60*24)

        return additional_info_data
    except SQLAlchemyError as e:
        logging.error("Error fetching additional_info: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.on_event("startup")
async def startup_event():
    await create_tables()
    logger.info("Starting up...")


@app.get("/scrape", include_in_schema=False)
async def scrape(scrape_limit: int = 10, db: AsyncSession = Depends(get_db), redis_cache: RedisCache = Depends(RedisCache)):
    playwright_setup = PlaywrightSetup(URL_TO_SCRAP)
    page = await playwright_setup.setup()

    try:
        custom_scraper = CustomScraper(
            URL_TO_SCRAP, scrape_limit, redis_cache, db, page, playwright_setup.browser)
        await custom_scraper.scrape_tools()
    finally:
        await playwright_setup.teardown()

    return {"message": "Scraping started"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
