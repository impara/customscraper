from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from model import Base
import logging
import os

# Use os.environ instead of os.getenv
DATABASE_URL = os.environ["DATABASE_URL"]

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
        await session.close()  # Close the session as soon as you're done with it


async def create_tables():
    try:
        # Create the tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            logging.info(
                "Database connection successful for create_async_engine!")
    except Exception as e:
        logging.error(
            "Database connection failed for create_async_engine. Error: %s", str(e))
        # Don't re-raise the exception
