from abc import ABC, abstractmethod
from typing import Dict, List

class DatabaseAdapter(ABC):
    """Abstract base class for database adapters"""
    
    @abstractmethod
    async def connect(self):
        """Connect to the database"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Disconnect from the database"""
        pass
    
    @abstractmethod
    async def insert_lead(self, lead_data: Dict):
        """Insert a new lead into the database"""
        pass
    
    @abstractmethod
    async def insert_campaign(self, campaign_data: Dict):
        """Insert a new campaign into the database"""
        pass
    
    @abstractmethod
    async def get_leads(self) -> List[Dict]:
        """Retrieve all leads from the database"""
        pass
    
    @abstractmethod
    async def get_campaigns(self) -> List[Dict]:
        """Retrieve all campaigns from the database"""
        pass