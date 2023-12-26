import aioredis
import orjson  # Import orjson
import os

REDIS_URL = os.environ["REDIS_URL"]  # Use os.environ instead of os.getenv


class RedisCache:
    _instance = None  # Class attribute to hold the singleton instance

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisCache, cls).__new__(cls)
            # Initialize the Redis connection only once
            cls._instance.redis_pool = aioredis.from_url(REDIS_URL)
        return cls._instance

    async def get_from_redis(self, key):
        value = await self.redis_pool.get(key)
        return orjson.loads(value) if value else None  # Use orjson.loads()

    async def set_to_redis(self, key, value, expire=None):
        # Use orjson.dumps()
        await self.redis_pool.set(key, orjson.dumps(value), ex=expire)

    async def mget(self, *keys):
        values = await self.redis_pool.execute_command('MGET', *keys)
        # Use orjson.loads()
        return [orjson.loads(value) if value else None for value in values]

    async def get_keys(self, pattern="*"):
        keys = await self.redis_pool.execute_command('KEYS', pattern)
        return keys

    async def exists(self, key):
        return await self.redis_pool.exists(key)
