from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os
# Create the engine and the sessionmaker outside of the function
engine = create_async_engine(
    os.getenv("DATABASE_URL"), echo=False,
)
async_session = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession)


async def create_async_db_connection():
    # Create a new session
    session = async_session()

    # Test the connection
    async with session.begin():
        await session.execute(text("SELECT 1"))

    return session
