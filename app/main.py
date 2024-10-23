
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