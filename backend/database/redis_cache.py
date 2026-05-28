"""Redis cache setup and utilities."""
import json
import logging
from functools import wraps
from typing import Optional, Any
import redis.asyncio as redis
from backend.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

redis_client: Optional[redis.Redis] = None

async def init_redis() -> None:
    """Initialize Redis client."""
    global redis_client
    try:
        redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        await redis_client.ping()
        logger.info("Redis connection established successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        redis_client = None

async def get_redis() -> Optional[redis.Redis]:
    """Get the Redis client instance."""
    if redis_client is None:
        await init_redis()
    return redis_client

async def close_redis() -> None:
    """Close Redis connection."""
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None
        logger.info("Redis connection closed.")

def cache_response(ttl_seconds: int = 300):
    """Decorator to cache function results in Redis."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            client = await get_redis()
            if not client:
                return await func(*args, **kwargs)

            # Create a simple cache key based on function name and arguments
            key_parts = [func.__name__]
            key_parts.extend([str(a) for a in args])
            key_parts.extend([f"{k}={v}" for k, v in sorted(kwargs.items())])
            cache_key = ":".join(key_parts)

            # Try to get from cache
            try:
                cached_data = await client.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
            except Exception as e:
                logger.warning(f"Redis cache read error: {e}")

            # Execute function
            result = await func(*args, **kwargs)

            # Save to cache
            try:
                if result is not None:
                    # Convert Pydantic models to dict if needed before caching
                    cache_val = result.model_dump() if hasattr(result, "model_dump") else result
                    await client.setex(cache_key, ttl_seconds, json.dumps(cache_val))
            except Exception as e:
                logger.warning(f"Redis cache write error: {e}")

            return result
        return wrapper
    return decorator
