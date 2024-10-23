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
