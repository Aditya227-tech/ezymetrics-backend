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