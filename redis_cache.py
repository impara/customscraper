import aioredis
import json
import os

REDIS_URL = os.environ["REDIS_URL"]  # Use os.environ instead of os.getenv


class RedisCache:
    def __init__(self):
        # Create the Redis connection in the constructor
        self.redis_pool = aioredis.from_url(REDIS_URL, encoding="utf-8")

    async def get_from_redis(self, key):
        value = await self.redis_pool.get(key)
        return json.loads(value) if value else None

    async def set_to_redis(self, key, value, expire=60 * 60 * 24):
        await self.redis_pool.set(key, json.dumps(value), ex=expire)

    async def mget(self, *keys):
        values = await self.redis_pool.execute_command('MGET', *keys)
        return [json.loads(value) if value else None for value in values]

    async def get_keys(self, pattern="*"):
        keys = await self.redis_pool.execute_command('KEYS', pattern)
        return keys

    async def exists(self, key):
        return await self.redis_pool.exists(key)
