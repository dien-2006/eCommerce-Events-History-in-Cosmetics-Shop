import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change_me")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ.get('DATABASE_USER')}:{os.environ.get('DATABASE_PASSWORD')}"
    f"@{os.environ.get('DATABASE_HOST')}:{os.environ.get('DATABASE_PORT')}/{os.environ.get('DATABASE_DB')}"
)

# Redis cache / async (simple demo config)
REDIS_HOST = os.environ.get("REDIS_HOST", "superset-redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
}
DATA_CACHE_CONFIG = CACHE_CONFIG

FEATURE_FLAGS = {
    "DASHBOARD_CROSS_FILTERS": True,
}