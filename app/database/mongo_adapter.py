# app/database/mongo_adapter.py
from motor.motor_asyncio import AsyncIOMotorClient # type: ignore
from datetime import datetime
from typing import List, Dict
import logging
from .base import DatabaseAdapter

class MongoAdapter(DatabaseAdapter):
    def __init__(self, connection_url: str):
        self.client = AsyncIOMotorClient(connection_url)
        self.db = self.client.ezymetrics
        self.leads = self.db.leads
        self.campaigns = self.db.campaigns
        self.logger = logging.getLogger(__name__)

    async def connect(self):
        try:
            # Verify connection
            await self.client.admin.command('ping')
            self.logger.info("Connected to MongoDB")
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    async def disconnect(self):
        try:
            self.client.close()
            self.logger.info("Disconnected from MongoDB")
        except Exception as e:
            self.logger.error(f"Failed to disconnect from MongoDB: {str(e)}")
            raise

    async def insert_lead(self, lead_data: Dict):
        try:
            # Ensure created_at is a datetime object
            if 'created_at' not in lead_data:
                lead_data['created_at'] = datetime.utcnow()
            await self.leads.insert_one(lead_data)
        except Exception as e:
            self.logger.error(f"Failed to insert lead: {str(e)}")
            raise

    async def insert_campaign(self, campaign_data: Dict):
        try:
            # Ensure dates are datetime objects
            for field in ['start_date', 'end_date']:
                if field in campaign_data and not isinstance(campaign_data[field], datetime):
                    campaign_data[field] = datetime.fromisoformat(str(campaign_data[field]))
            await self.campaigns.insert_one(campaign_data)
        except Exception as e:
            self.logger.error(f"Failed to insert campaign: {str(e)}")
            raise

    async def get_leads(self) -> List[Dict]:
        try:
            cursor = self.leads.find({})
            leads = await cursor.to_list(length=None)
            # Convert ObjectId to string for JSON serialization
            for lead in leads:
                lead['id'] = str(lead.pop('_id'))
            return leads
        except Exception as e:
            self.logger.error(f"Failed to fetch leads: {str(e)}")
            raise

    async def get_campaigns(self) -> List[Dict]:
        try:
            cursor = self.campaigns.find({})
            campaigns = await cursor.to_list(length=None)
            # Convert ObjectId to string for JSON serialization
            for campaign in campaigns:
                campaign['id'] = str(campaign.pop('_id'))
            return campaigns
        except Exception as e:
            self.logger.error(f"Failed to fetch campaigns: {str(e)}")
            raise