"""Database connection and session management."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
from backend.config import get_settings

settings = get_settings()

# Create sync database engine (strip asyncpg for sync usage)
sync_url = settings.database_url.replace("+asyncpg", "")
engine = create_engine(
    sync_url,
    echo=settings.debug,
    pool_size=10,
    max_overflow=20,
)

# Session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def get_db() -> Generator[Session, None, None]:
    """Dependency for getting database session in routes."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
