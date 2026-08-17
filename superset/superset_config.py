import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://finance:finance@postgres:5432/finance",
)

# Allow all origins during development — restrict in production
WTF_CSRF_ENABLED = True
SESSION_COOKIE_SAMESITE = "Lax"

# Cache (in-memory for single-node dev)
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}

# Enable async queries for large datasets
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "GENERIC_CHART_AXES": True,
}

# Row limit for SQL Lab queries
SQL_MAX_ROW = 100_000
DISPLAY_MAX_ROW = 10_000
