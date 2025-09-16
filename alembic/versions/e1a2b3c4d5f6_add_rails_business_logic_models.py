"""Add Rails business logic models: user_login_audits, org_custodians, billing_accounts, subscriptions, notification_channel_settings

Revision ID: e1a2b3c4d5f6
Revises: f4e3d2c1b0a9
Create Date: 2025-09-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'e1a2b3c4d5f6'
down_revision = 'f4e3d2c1b0a9'
branch_labels = None
depends_on = None


def upgrade():
    # UserLoginAudit table
    op.create_table('user_login_audits',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('uuid', sa.String(36), unique=True, index=True),
        sa.Column('attempt_type', sa.Enum('SUCCESS', 'FAILURE', 'LOCKOUT', 'MFA_REQUIRED', 'MFA_SUCCESS', 'MFA_FAILURE', 'LOGOUT', 'SESSION_EXPIRED'), nullable=False, index=True),
        sa.Column('login_method', sa.Enum('PASSWORD', 'API_KEY', 'SSO', 'OAUTH', 'MFA', 'SERVICE_ACCOUNT', 'IMPERSONATION'), default='PASSWORD', index=True),
        sa.Column('email_attempted', sa.String(255), index=True),
        sa.Column('ip_address', sa.String(45), index=True),
        sa.Column('user_agent', sa.Text()),
        sa.Column('session_id', sa.String(255), index=True),
        sa.Column('failure_reason', sa.String(255)),
        sa.Column('is_suspicious', sa.Boolean(), default=False, index=True),
        sa.Column('risk_score', sa.Integer(), default=0, index=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id'), index=True),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('orgs.id'), index=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('attempted_at', sa.DateTime(), default=sa.func.now(), index=True),
    )
    
    # OrgCustodian table
    op.create_table('org_custodians',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('orgs.id'), nullable=False, index=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False, index=True),
        sa.Column('assigned_by', sa.Integer(), sa.ForeignKey('users.id'), index=True),
        sa.Column('role_level', sa.String(50), default='CUSTODIAN', index=True),
        sa.Column('permissions', sa.String(500)),
        sa.Column('is_active', sa.Boolean(), default=True, index=True),
        sa.Column('can_manage_users', sa.Boolean(), default=True),
        sa.Column('can_manage_data', sa.Boolean(), default=True),
        sa.Column('can_manage_billing', sa.Boolean(), default=False),
        sa.Column('assigned_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('expires_at', sa.DateTime(), index=True),
        sa.Column('revoked_at', sa.DateTime(), index=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )
    
    # BillingAccount table
    op.create_table('billing_accounts',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('orgs.id'), nullable=False, index=True),
        sa.Column('billing_contact_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=True, index=True),
        sa.Column('account_number', sa.String(50), unique=True, nullable=False, index=True),
        sa.Column('external_id', sa.String(100), unique=True, nullable=True, index=True),
        sa.Column('status', sa.Enum('ACTIVE', 'SUSPENDED', 'CANCELLED', 'PAST_DUE', 'TRIAL'), default='TRIAL', index=True),
        sa.Column('billing_cycle', sa.Enum('MONTHLY', 'QUARTERLY', 'YEARLY', 'CUSTOM'), default='MONTHLY', index=True),
        sa.Column('currency', sa.String(3), default='USD', index=True),
        sa.Column('billing_email', sa.String(254), nullable=False),
        sa.Column('company_name', sa.String(255)),
        sa.Column('billing_address', sa.JSON()),
        sa.Column('tax_id', sa.String(50)),
        sa.Column('primary_payment_method', sa.Enum('CREDIT_CARD', 'BANK_TRANSFER', 'INVOICE', 'PAYPAL', 'CRYPTO'), nullable=True),
        sa.Column('payment_method_details', sa.JSON()),
        sa.Column('auto_pay_enabled', sa.Boolean(), default=True),
        sa.Column('current_balance', sa.Numeric(precision=10, scale=2), default=0),
        sa.Column('credit_limit', sa.Numeric(precision=10, scale=2), default=0),
        sa.Column('total_paid', sa.Numeric(precision=10, scale=2), default=0),
        sa.Column('total_outstanding', sa.Numeric(precision=10, scale=2), default=0),
        sa.Column('monthly_limit', sa.Numeric(precision=10, scale=2)),
        sa.Column('usage_threshold_warning', sa.Integer(), default=80),
        sa.Column('usage_threshold_limit', sa.Integer(), default=100),
        sa.Column('trial_start_date', sa.DateTime()),
        sa.Column('trial_end_date', sa.DateTime()),
        sa.Column('promotional_credits', sa.Numeric(precision=10, scale=2), default=0),
        sa.Column('discount_codes', sa.JSON()),
        sa.Column('invoice_delivery_method', sa.String(20), default='EMAIL'),
        sa.Column('payment_terms_days', sa.Integer(), default=30),
        sa.Column('grace_period_days', sa.Integer(), default=7),
        sa.Column('account_manager_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('notes', sa.Text()),
        sa.Column('tags', sa.String(500)),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('activated_at', sa.DateTime()),
        sa.Column('suspended_at', sa.DateTime()),
        sa.Column('last_payment_at', sa.DateTime()),
        sa.Column('next_billing_date', sa.DateTime(), index=True),
    )
    
    # Subscription table
    op.create_table('subscriptions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('orgs.id'), nullable=False, index=True),
        sa.Column('billing_account_id', sa.Integer(), sa.ForeignKey('billing_accounts.id'), nullable=False, index=True),
        sa.Column('plan_id', sa.String(100), nullable=False, index=True),
        sa.Column('subscription_type', sa.Enum('BASIC', 'PROFESSIONAL', 'ENTERPRISE', 'CUSTOM', 'ADD_ON'), nullable=False, index=True),
        sa.Column('status', sa.Enum('ACTIVE', 'CANCELLED', 'SUSPENDED', 'EXPIRED', 'PENDING', 'TRIAL'), default='PENDING', index=True),
        sa.Column('base_price', sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column('current_price', sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column('usage_limits', sa.JSON()),
        sa.Column('current_usage', sa.JSON()),
        sa.Column('auto_renew', sa.Boolean(), default=True),
        sa.Column('discount_percentage', sa.Numeric(precision=5, scale=2), default=0),
        sa.Column('discount_amount', sa.Numeric(precision=10, scale=2), default=0),
        sa.Column('discount_expires_at', sa.DateTime()),
        sa.Column('trial_end_date', sa.DateTime(), index=True),
        sa.Column('current_period_start', sa.DateTime(), index=True),
        sa.Column('current_period_end', sa.DateTime(), index=True),
        sa.Column('cancelled_at', sa.DateTime()),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )
    
    # NotificationChannelSetting table
    op.create_table('notification_channel_settings',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False, index=True),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('orgs.id'), index=True),
        sa.Column('channel', sa.Enum('EMAIL', 'SMS', 'PUSH', 'SLACK', 'WEBHOOK', 'IN_APP', 'DESKTOP'), nullable=False, index=True),
        sa.Column('notification_type', sa.Enum('SYSTEM_ALERT', 'DATA_PIPELINE', 'SECURITY', 'BILLING', 'USER_ACTIVITY', 'API_USAGE', 'MAINTENANCE', 'MARKETING'), nullable=False, index=True),
        sa.Column('frequency', sa.Enum('REAL_TIME', 'HOURLY', 'DAILY', 'WEEKLY', 'MONTHLY', 'NEVER'), default='REAL_TIME', index=True),
        sa.Column('is_enabled', sa.Boolean(), default=True, index=True),
        sa.Column('is_muted', sa.Boolean(), default=False, index=True),
        sa.Column('muted_until', sa.DateTime(), nullable=True),
        sa.Column('delivery_address', sa.String(255)),
        sa.Column('delivery_config', sa.JSON()),
        sa.Column('include_details', sa.Boolean(), default=True),
        sa.Column('include_attachments', sa.Boolean(), default=False),
        sa.Column('template_id', sa.String(100)),
        sa.Column('filter_rules', sa.JSON()),
        sa.Column('priority_threshold', sa.String(20), default='LOW'),
        sa.Column('keyword_filters', sa.Text()),
        sa.Column('quiet_hours_enabled', sa.Boolean(), default=False),
        sa.Column('quiet_hours_start', sa.String(8)),
        sa.Column('quiet_hours_end', sa.String(8)),
        sa.Column('quiet_hours_timezone', sa.String(50), default='UTC'),
        sa.Column('rate_limit_enabled', sa.Boolean(), default=False),
        sa.Column('max_notifications_per_hour', sa.Integer(), default=60),
        sa.Column('max_notifications_per_day', sa.Integer(), default=500),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('last_notification_at', sa.DateTime()),
        sa.Column('last_delivery_attempt_at', sa.DateTime()),
    )


def downgrade():
    op.drop_table('notification_channel_settings')
    op.drop_table('subscriptions')
    op.drop_table('billing_accounts')
    op.drop_table('org_custodians')
    op.drop_table('user_login_audits')