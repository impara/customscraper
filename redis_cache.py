import aioredis
import json
import os

REDIS_URL = os.getenv("REDIS_URL")


class RedisCache:
    def __init__(self):
        self.redis_pool = None

    async def get_redis_pool(self):
        if self.redis_pool is None:
            self.redis_pool = await aioredis.from_url(REDIS_URL, encoding="utf-8")
        return self.redis_pool

    async def get_from_redis(self, key):
        if not self.redis_pool:
            await self.get_redis_pool()
        value = await self.redis_pool.get(key)
        return json.loads(value) if value else None

    async def set_to_redis(self, key, value, expire=60 * 60 * 24):
        if not self.redis_pool:
            await self.get_redis_pool()
        await self.redis_pool.set(key, json.dumps(value), ex=expire)

    async def mget(self, *keys):
        if not self.redis_pool:
            await self.get_redis_pool()
        values = await self.redis_pool.execute_command('MGET', *keys)
        return [json.loads(value) if value else None for value in values]
