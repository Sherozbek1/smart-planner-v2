# db.py

import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# remove NullPool to enable normal pooling for speed
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_timeout=30,
)

AsyncSessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

async def init_db():
    """Create all tables defined in db_models.py"""
    from db_models import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
