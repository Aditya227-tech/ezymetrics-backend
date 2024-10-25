from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession # type: ignore
from sqlalchemy.orm import sessionmaker # type: ignore
from sqlalchemy import Column, Integer, String, Float, DateTime, select # type: ignore
from sqlalchemy.ext.declarative import declarative_base # type: ignore
from datetime import datetime
from typing import List, Dict, Optional
import logging
from .base import DatabaseAdapter # type: ignore

Base = declarative_base()

class Lead(Base):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    email = Column(String)
    source = Column(String)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

class Campaign(Base):
    __tablename__ = "campaigns"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    platform = Column(String)
    budget = Column(Float)
    spend = Column(Float)
    impressions = Column(Integer)
    clicks = Column(Integer)
    conversions = Column(Integer)
    start_date = Column(DateTime)
    end_date = Column(DateTime)

class PostgresAdapter(DatabaseAdapter):
    def __init__(self, connection_url: str):
        self.engine = create_async_engine(connection_url)
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        self.logger = logging.getLogger(__name__)

    async def connect(self):
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise

    async def disconnect(self):
        try:
            await self.engine.dispose()
        except Exception as e:
            self.logger.error(f"Failed to disconnect from database: {str(e)}")
            raise

    async def insert_lead(self, lead_data: Dict):
        try:
            async with self.SessionLocal() as session:
                async with session.begin():
                    lead = Lead(**lead_data)
                    session.add(lead)
        except Exception as e:
            self.logger.error(f"Failed to insert lead: {str(e)}")
            raise

    async def insert_campaign(self, campaign_data: Dict):
        try:
            async with self.SessionLocal() as session:
                async with session.begin():
                    campaign = Campaign(**campaign_data)
                    session.add(campaign)
        except Exception as e:
            self.logger.error(f"Failed to insert campaign: {str(e)}")
            raise

    async def get_leads(self) -> List[Dict]:
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(Lead))
                leads = result.scalars().all()
                return [
                    {
                        "id": lead.id,
                        "name": lead.name,
                        "email": lead.email,
                        "source": lead.source,
                        "status": lead.status,
                        "created_at": lead.created_at
                    }
                    for lead in leads
                ]
        except Exception as e:
            self.logger.error(f"Failed to fetch leads: {str(e)}")
            raise

    async def get_campaigns(self) -> List[Dict]:
        try:
            async with self.SessionLocal() as session:
                result = await session.execute(select(Campaign))
                campaigns = result.scalars().all()
                return [
                    {
                        "id": campaign.id,
                        "name": campaign.name,
                        "platform": campaign.platform,
                        "budget": campaign.budget,
                        "spend": campaign.spend,
                        "impressions": campaign.impressions,
                        "clicks": campaign.clicks,
                        "conversions": campaign.conversions,
                        "start_date": campaign.start_date,
                        "end_date": campaign.end_date
                    }
                    for campaign in campaigns
                ]
        except Exception as e:
            self.logger.error(f"Failed to fetch campaigns: {str(e)}")
            raise