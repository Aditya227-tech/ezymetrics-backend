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
