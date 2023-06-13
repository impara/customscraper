from typing import Optional
import uvicorn
from fastapi import FastAPI
from scraper import startup_event_scrape, CustomScraper
import logging
from database import create_tables, get_db
from fastapi import Depends
from sqlalchemy import select, func
from sqlalchemy.exc import SQLAlchemyError
from typing import List
from fastapi import HTTPException
from model import ToolTable, ToolResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from redis_cache import RedisCache
import os

URL_TO_SCRAP = os.getenv("URL_TO_SCRAP")
# Create a custom logger
logger = logging.getLogger(__name__)
# Configure logger
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

app = FastAPI()


@app.get("/tools", response_model=List[ToolResponse])
async def get_tools(
    skip: int = 0, limit: int = 10, additional_info: str = None, pricing_model: str = None, db: AsyncSession = Depends(get_db), redis_cache: RedisCache = Depends(RedisCache)
):
    try:
        cache_key = f"tools:{skip}:{limit}:{additional_info}:{pricing_model}"
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
            result = await db.execute(query)
            tools = result.scalars().all()
            tools_data = [tool.to_dict() for tool in tools]
            # Set a key with a TTL (Time To Live) of 24 hours
            await redis_cache.set_to_redis(cache_key, tools_data, expire=60*60*24)

        return tools_data
    except SQLAlchemyError as e:
        logging.error("Error fetching tools: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.on_event("startup")
async def startup_event():
    await create_tables()
    logger.info("Starting up...")


@app.get("/scrape", include_in_schema=False)
async def scrape(scrape_limit: int = 10, db: AsyncSession = Depends(get_db), redis_cache: RedisCache = Depends(RedisCache)):
    custom_scraper = CustomScraper(
        URL_TO_SCRAP, scrape_limit, redis_cache, db)
    await startup_event_scrape(scrape_limit, custom_scraper)
    return {"message": "Scraping started"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
