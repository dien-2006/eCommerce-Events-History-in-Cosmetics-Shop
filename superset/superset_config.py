import os
from urllib.parse import quote_plus

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:"
    + quote_plus(os.environ["SUPERSET_DB_PASSWORD"])
    + "@postgres:5432/superset"
)
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
# Cache disabled so an atomic release switch is visible immediately.
CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
FEATURE_FLAGS = {"DASHBOARD_CROSS_FILTERS": True}
ENABLE_PROXY_FIX = True
TALISMAN_ENABLED = True
TALISMAN_CONFIG = {
    "force_https": False,  # terminate TLS at the host reverse proxy
    "content_security_policy": {
        "default-src": ["'self'"],
        "img-src": ["'self'", "data:", "blob:"],
        "worker-src": ["'self'", "blob:"],
        "connect-src": ["'self'"],
        "object-src": ["'none'"],
        "style-src": ["'self'", "'unsafe-inline'"],
        "script-src": ["'self'", "'strict-dynamic'"],
    },
    "content_security_policy_nonce_in": ["script-src"],
}
