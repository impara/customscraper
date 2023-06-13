from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from model import Base
import logging
import os

DATABASE_URL = os.getenv("DATABASE_URL")

# Define the engine globally
engine = create_async_engine(DATABASE_URL, future=True)

# Create async sessionmaker
async_session = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession)


async def get_db():
    session = async_session()
    try:
        yield session
    finally:
        await session.close()


async def create_tables():
    # Define the engine variable outside the try block with a default value
    try:
        # Create the tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            logging.info(
                "Database connection successful for create_async_engine!")
    except Exception as e:
        logging.error(
            "Database connection failed for create_async_engine. Error: %s", str(e))
        raise
