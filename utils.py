from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os

# Use the same engine and sessionmaker as in database.py
from database import engine, async_session


async def create_async_db_connection():
    # Create a new session
    session = async_session()

    # Test the connection
    async with session.begin():
        await session.execute(text("SELECT 1"))

    return session
