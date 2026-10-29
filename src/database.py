import os
from contextlib import asynccontextmanager
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from datetime import datetime
from logger import get_trading_logger

logger = get_trading_logger()

# استفاده از SQLite به جای PostgreSQL برای اجرای بدون دیتابیس خارجی
DATABASE_URL = "sqlite+aiosqlite:///./trading_bot.db"

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

class TradingSignal(Base):
    __tablename__ = 'trading_signals'
    id = Column(Integer, primary_key=True, index=True); symbol = Column(String); type = Column(String); entry_price = Column(Float); stop_loss = Column(Float); take_profit_1 = Column(Float); take_profit_2 = Column(Float); strategy = Column(String); session = Column(String); timestamp = Column(DateTime, default=datetime.utcnow)

class Trade(Base):
    __tablename__ = 'trades'
    id = Column(Integer, primary_key=True, index=True); symbol = Column(String); type = Column(String); entry_price = Column(Float); exit_price = Column(Float, nullable=True); status = Column(String); timestamp = Column(DateTime, default=datetime.utcnow)

class UserInteraction(Base):
    __tablename__ = 'user_interactions'
    id = Column(Integer, primary_key=True, index=True); user_id = Column(String); text = Column(String); timestamp = Column(DateTime, default=datetime.utcnow)

@asynccontextmanager
async def get_db_session():
    async with AsyncSessionLocal() as session: yield session

async def init_database():
    async with engine.begin() as conn: await conn.run_sync(Base.metadata.create_all)

async def log_interaction(user_id: str, text: str):
    try:
        async with get_db_session() as session:
            session.add(UserInteraction(user_id=str(user_id), text=text)); await session.commit()
    except Exception as e: logger.log_error(e, {"context": "log_interaction"})
