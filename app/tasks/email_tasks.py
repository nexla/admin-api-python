"""
Email tasks - Replaces Rails ActionMailer functionality.
"""

from celery import current_app as celery_app
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from typing import List, Dict, Any
import logging
from ..config import settings

logger = logging.getLogger(__name__)

# Email configuration (replaces Rails ActionMailer)
email_config = ConnectionConfig(
    MAIL_USERNAME=settings.SMTP_USERNAME,
    MAIL_PASSWORD=settings.SMTP_PASSWORD,
    MAIL_FROM=settings.SMTP_FROM_EMAIL,
    MAIL_PORT=settings.SMTP_PORT,
    MAIL_SERVER=settings.SMTP_HOST,
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True,
)

fastmail = FastMail(email_config)

@celery_app.task(bind=True)
def send_email(self, recipients: List[str], subject: str, body: str, html_body: str = None):
    """
    Send email task.
    Replaces Rails ActionMailer deliver_later functionality.
    """
    try:
        message = MessageSchema(
            subject=subject,
            recipients=recipients,
            body=body,
            html=html_body,
        )
        
        # This would be async in a real FastAPI context
        # For Celery, we need to handle it differently
        logger.info(f"Sending email to {recipients}: {subject}")
        
        # TODO: Implement actual email sending
        # In a real implementation, you'd use smtplib or similar
        
        return {"status": "sent", "recipients": len(recipients)}
        
    except Exception as exc:
        logger.error(f"Failed to send email: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)

@celery_app.task
def send_welcome_email(user_email: str, user_name: str):
    """Send welcome email to new users."""
    subject = "Welcome to Admin API"
    body = f"Hello {user_name},\n\nWelcome to Admin API!"
    html_body = f"""
    <html>
    <body>
        <h1>Welcome {user_name}!</h1>
        <p>Thank you for joining Admin API.</p>
    </body>
    </html>
    """
    
    return send_email.delay([user_email], subject, body, html_body)

@celery_app.task
def send_password_reset_email(user_email: str, reset_token: str):
    """Send password reset email."""
    reset_url = f"{settings.FRONTEND_URL}/reset-password?token={reset_token}"
    subject = "Password Reset Request"
    body = f"Click this link to reset your password: {reset_url}"
    html_body = f"""
    <html>
    <body>
        <h2>Password Reset</h2>
        <p>Click the link below to reset your password:</p>
        <a href="{reset_url}">Reset Password</a>
        <p>If you didn't request this, please ignore this email.</p>
    </body>
    </html>
    """
    
    return send_email.delay([user_email], subject, body, html_body)

@celery_app.task
def send_daily_reports():
    """Send daily reports to admins."""
    # TODO: Implement daily reporting logic
    logger.info("Generating daily reports...")
    return {"status": "completed", "reports_sent": 0}