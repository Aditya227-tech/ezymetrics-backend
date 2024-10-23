# app/database/postgres_adapter.py
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

# app/utils/data_generators.py
from datetime import datetime, timedelta
import random
from typing import List, Dict

def generate_dummy_leads(count: int = 10) -> List[Dict]:
    sources = ["Website", "LinkedIn", "Facebook", "Google"]
    statuses = ["New", "Contacted", "Qualified", "Converted"]
    
    return [
        {
            "name": f"Lead {i}",
            "email": f"lead{i}@example.com",
            "source": random.choice(sources),
            "status": random.choice(statuses),
            "created_at": datetime.utcnow() - timedelta(days=random.randint(0, 30))
        }
        for i in range(count)
    ]

def generate_dummy_campaigns(count: int = 5) -> List[Dict]:
    platforms = ["Facebook", "Google Ads", "LinkedIn", "Twitter"]
    
    return [
        {
            "name": f"Campaign {i}",
            "platform": random.choice(platforms),
            "budget": random.uniform(1000, 10000),
            "spend": random.uniform(500, 8000),
            "impressions": random.randint(10000, 100000),
            "clicks": random.randint(100, 5000),
            "conversions": random.randint(10, 500),
            "start_date": datetime.utcnow() - timedelta(days=random.randint(10, 60)),
            "end_date": datetime.utcnow() + timedelta(days=random.randint(0, 30))
        }
        for i in range(count)
    ]

# app/utils/metrics.py
from typing import List, Dict

def transform_lead_data(leads: List[Dict]) -> Dict:
    if not leads:
        return {
            "total_leads": 0,
            "leads_by_source": {},
            "leads_by_status": {},
            "conversion_rate": 0
        }
    
    total_leads = len(leads)
    leads_by_source = {}
    leads_by_status = {}
    converted_leads = 0
    
    for lead in leads:
        source = lead["source"]
        status = lead["status"]
        
        leads_by_source[source] = leads_by_source.get(source, 0) + 1
        leads_by_status[status] = leads_by_status.get(status, 0) + 1
        
        if status == "Converted":
            converted_leads += 1
    
    return {
        "total_leads": total_leads,
        "leads_by_source": leads_by_source,
        "leads_by_status": leads_by_status,
        "conversion_rate": (converted_leads / total_leads) * 100 if total_leads > 0 else 0
    }

def transform_campaign_data(campaigns: List[Dict]) -> Dict:
    if not campaigns:
        return {
            "total_campaigns": 0,
            "total_spend": 0,
            "total_impressions": 0,
            "total_clicks": 0,
            "total_conversions": 0,
            "avg_conversion_rate": 0,
            "avg_ctr": 0,
            "avg_cpc": 0
        }
    
    total_campaigns = len(campaigns)
    total_spend = sum(c["spend"] for c in campaigns)
    total_impressions = sum(c["impressions"] for c in campaigns)
    total_clicks = sum(c["clicks"] for c in campaigns)
    total_conversions = sum(c["conversions"] for c in campaigns)
    
    return {
        "total_campaigns": total_campaigns,
        "total_spend": total_spend,
        "total_impressions": total_impressions,
        "total_clicks": total_clicks,
        "total_conversions": total_conversions,
        "avg_conversion_rate": (total_conversions / total_clicks * 100) if total_clicks > 0 else 0,
        "avg_ctr": (total_clicks / total_impressions * 100) if total_impressions > 0 else 0,
        "avg_cpc": (total_spend / total_clicks) if total_clicks > 0 else 0
    }

# app/utils/email.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from typing import Optional
import logging

logger = logging.getLogger(__name__)

async def send_email_alert(
    subject: str,
    body: str,
    recipient: Optional[str] = None
) -> None:
    """Send email alerts for important notifications"""
    try:
        sender = os.getenv("EMAIL_SENDER")
        password = os.getenv("EMAIL_PASSWORD")
        recipient = recipient or os.getenv("DEFAULT_ALERT_RECIPIENT")
        
        if not all([sender, password, recipient]):
            logger.error("Missing email configuration")
            return
        
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = recipient
        msg["Subject"] = subject
        
        msg.attach(MIMEText(body, "plain"))
        
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.send_message(msg)
            
        logger.info(f"Alert email sent to {recipient}")
    except Exception as e:
        logger.error(f"Failed to send email alert: {str(e)}")

# app/main.py
from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends # type: ignore
from fastapi.responses import FileResponse # type: ignore
import pandas as pd
from typing import Dict
import os
import logging
from .database.base import DatabaseAdapter
from .database.postgres_adapter import PostgresAdapter
from .database.mongo_adapter import MongoAdapter
from .utils.data_generators import generate_dummy_leads, generate_dummy_campaigns
from .utils.metrics import transform_lead_data, transform_campaign_data
from .utils.email import send_email_alert

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="EzyMetrics Backend")

# Database configuration
DATABASE_TYPE = os.getenv("DATABASE_TYPE", "postgres")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@localhost/ezymetrics")

def get_db_adapter() -> DatabaseAdapter:
    if DATABASE_TYPE == "postgres":
        return PostgresAdapter(DATABASE_URL)
    elif DATABASE_TYPE == "mongodb":
        return MongoAdapter(DATABASE_URL)
    else:
        raise ValueError(f"Unsupported database type: {DATABASE_TYPE}")

async def get_db():
    db = get_db_adapter()
    try:
        await db.connect()
        yield db
    finally:
        await db.disconnect()

def cleanup_report_file(file_path: str) -> None:
    """Remove temporary report files"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logger.error(f"Failed to cleanup report file {file_path}: {str(e)}")

@app.post("/api/fetch-data")
async def fetch_data(
    background_tasks: BackgroundTasks,
    db: DatabaseAdapter = Depends(get_db)
):
    """Fetch and store dummy data from CRM and Marketing platforms"""
    try:
        leads_data = generate_dummy_leads()
        campaigns_data = generate_dummy_campaigns()
        
        for lead_data in leads_data:
            await db.insert_lead(lead_data)
        
        for campaign_data in campaigns_data:
            await db.insert_campaign(campaign_data)
        
        campaign_metrics = transform_campaign_data(campaigns_data)
        if campaign_metrics["avg_conversion_rate"] < 5:
            background_tasks.add_task(
                send_email_alert,
                "Low Conversion Rate Alert",
                f"Campaign conversion rate has dropped below 5%: {campaign_metrics['avg_conversion_rate']:.2f}%"
            )
        
        return {"message": "Data fetched and stored successfully"}
    except Exception as e:
        logger.error(f"Error in fetch_data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reports/{report_type}")
async def generate_report(
    report_type: str,
    background_tasks: BackgroundTasks,
    db: DatabaseAdapter = Depends(get_db)
):
    """Generate reports in CSV format"""
    report_path = None
    try:
        if report_type == "leads":
            leads = await db.get_leads()
            df = pd.DataFrame(leads)
            report_path = "leads_report.csv"
            df.to_csv(report_path, index=False)
            background_tasks.add_task(cleanup_report_file, report_path)
            return FileResponse(report_path, filename="leads_report.csv")
            
        elif report_type == "campaigns":
            campaigns = await db.get_campaigns()
            df = pd.DataFrame(campaigns)
            report_path = "campaigns_report.csv"
            df.to_csv(report_path, index=False)
            background_tasks.add_task(cleanup_report_file, report_path)
            return FileResponse(report_path, filename="campaigns_report.csv")
        
        else:
            raise HTTPException(status_code=400, detail="Invalid report type")
    except Exception as e:
        if report_path and os.path.exists(report_path):
            cleanup_report_file(report_path)
        logger.error(f"Error in generate_report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics")
async def get_metrics(db: DatabaseAdapter = Depends(get_db)):
    """Get transformed metrics from the stored data"""
    try:
        leads = await db.get_leads()
        campaigns = await db.get_campaigns()
        
        return {
            "lead_metrics": transform_lead_data(leads),
            "campaign_metrics": transform_campaign_data(campaigns)
        }
    except Exception as e:
        logger.error(f"Error in get_metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn # type: ignore
    uvicorn.run(app, host="0.0.0.0", port=8000)