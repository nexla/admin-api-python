"""Add missing critical tables: api_keys, permissions, sessions, teams, webhooks

Revision ID: f4e3d2c1b0a9
Revises: d84adbacc388
Create Date: 2025-09-14 16:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'f4e3d2c1b0a9'
down_revision = 'd84adbacc388'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create api_keys table
    op.create_table('api_keys',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('key_id', sa.CHAR(36), unique=True, nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('key_prefix', sa.String(length=20), nullable=False),
        sa.Column('key_hash', sa.String(length=128), nullable=False, index=True),
        sa.Column('key_suffix', sa.String(length=10), nullable=False),
        sa.Column('api_key_type', sa.Enum('FULL_ACCESS', 'READ_ONLY', 'WRITE_ONLY', 'SERVICE_ACCOUNT', 'ADMIN', 'CUSTOM', name='apikeytype'), nullable=False, default='READ_ONLY'),
        sa.Column('status', sa.Enum('ACTIVE', 'INACTIVE', 'REVOKED', 'EXPIRED', 'SUSPENDED', 'COMPROMISED', name='apikeystatus'), nullable=False, default='ACTIVE'),
        sa.Column('scope', sa.Enum('ORG', 'PROJECT', 'RESOURCE', 'ADMIN', 'SYSTEM', name='apikeyscope'), nullable=False, default='ORG'),
        sa.Column('environment', sa.Enum('DEVELOPMENT', 'STAGING', 'PRODUCTION', 'TESTING', name='apikeyenvironment'), nullable=False, default='DEVELOPMENT'),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('orgs.id'), nullable=False),
        sa.Column('project_id', sa.Integer(), sa.ForeignKey('projects.id'), nullable=True),
        sa.Column('permissions', sa.JSON(), default=[]),
        sa.Column('allowed_ips', sa.JSON(), default=[]),
        sa.Column('allowed_domains', sa.JSON(), default=[]),
        sa.Column('allowed_endpoints', sa.JSON(), default=[]),
        sa.Column('rate_limit_requests', sa.Integer(), default=1000),
        sa.Column('rate_limit_window_seconds', sa.Integer(), default=3600),
        sa.Column('current_rate_count', sa.Integer(), default=0),
        sa.Column('rate_limit_reset_at', sa.DateTime(), nullable=True),
        sa.Column('usage_count', sa.BigInteger(), default=0),
        sa.Column('last_used_at', sa.DateTime(), nullable=True),
        sa.Column('last_used_ip', sa.String(length=45), nullable=True),
        sa.Column('last_used_user_agent', sa.String(length=500), nullable=True),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.Column('revoked_at', sa.DateTime(), nullable=True),
        sa.Column('revoked_by', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('revocation_reason', sa.String(length=500), nullable=True),
        sa.Column('rotation_schedule_days', sa.Integer(), nullable=True),
        sa.Column('last_rotated_at', sa.DateTime(), nullable=True),
        sa.Column('next_rotation_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_api_keys_id', 'id'),
        sa.Index('ix_api_keys_key_hash', 'key_hash'),
        sa.UniqueConstraint('key_id')
    )
    
    # Create permissions table
    op.create_table('permissions',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('name', sa.String(length=255), nullable=False, unique=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('resource_type', sa.String(length=100), nullable=True),
        sa.Column('action', sa.String(length=100), nullable=True),
        sa.Column('category', sa.String(length=100), nullable=True),
        sa.Column('is_system', sa.Boolean(), default=False),
        sa.Column('is_active', sa.Boolean(), default=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_permissions_id', 'id'),
        sa.UniqueConstraint('name')
    )
    
    # Create sessions table
    op.create_table('sessions',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('session_id', sa.String(length=255), nullable=False, unique=True, index=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('orgs.id'), nullable=True),
        sa.Column('status', sa.Enum('ACTIVE', 'EXPIRED', 'REVOKED', 'TERMINATED', name='sessionstatus'), nullable=False, default='ACTIVE'),
        sa.Column('session_data', sa.JSON(), default={}),
        sa.Column('ip_address', sa.String(length=45), nullable=True),
        sa.Column('user_agent', sa.String(length=500), nullable=True),
        sa.Column('device_fingerprint', sa.String(length=255), nullable=True),
        sa.Column('last_activity_at', sa.DateTime(), nullable=True, index=True),
        sa.Column('expires_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('remember_me', sa.Boolean(), default=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_sessions_id', 'id'),
        sa.Index('ix_sessions_session_id', 'session_id'),
        sa.Index('ix_sessions_last_activity_at', 'last_activity_at'),
        sa.Index('ix_sessions_expires_at', 'expires_at'),
        sa.UniqueConstraint('session_id')
    )
    
    # Create teams table (if not exists from migration)
    op.create_table('teams',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('slug', sa.String(length=255), nullable=True, unique=True),
        sa.Column('status', sa.Enum('ACTIVE', 'INACTIVE', 'ARCHIVED', 'SUSPENDED', name='teamstatus'), nullable=False, default='ACTIVE'),
        sa.Column('visibility', sa.Enum('PUBLIC', 'PRIVATE', 'ORG_WIDE', name='teamvisibility'), nullable=False, default='PRIVATE'),
        sa.Column('owner_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('orgs.id'), nullable=False),
        sa.Column('member_count', sa.Integer(), default=0),
        sa.Column('max_members', sa.Integer(), nullable=True),
        sa.Column('settings', sa.JSON(), default={}),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_teams_id', 'id'),
        sa.Index('ix_teams_owner_id', 'owner_id'),
        sa.Index('ix_teams_org_id', 'org_id'),
        sa.UniqueConstraint('slug')
    )
    
    # Create team_invitations table
    op.create_table('team_invitations',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('team_id', sa.Integer(), sa.ForeignKey('teams.id'), nullable=False),
        sa.Column('inviter_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('invitee_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('email', sa.String(length=255), nullable=True),
        sa.Column('status', sa.Enum('PENDING', 'ACCEPTED', 'DECLINED', 'CANCELLED', 'EXPIRED', name='teaminvitationstatus'), nullable=False, default='PENDING'),
        sa.Column('role', sa.Enum('MEMBER', 'ADMIN', 'OWNER', name='teaminvitationrole'), nullable=False, default='MEMBER'),
        sa.Column('token', sa.String(length=255), nullable=True, unique=True),
        sa.Column('message', sa.Text(), nullable=True),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.Column('accepted_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_team_invitations_id', 'id'),
        sa.Index('ix_team_invitations_team_id', 'team_id'),
        sa.Index('ix_team_invitations_token', 'token'),
        sa.UniqueConstraint('token')
    )
    
    # Create webhooks table
    op.create_table('webhooks',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('url', sa.String(length=2000), nullable=False),
        sa.Column('http_method', sa.Enum('GET', 'POST', 'PUT', 'PATCH', 'DELETE', name='webhookhttpmethod'), nullable=False, default='POST'),
        sa.Column('status', sa.Enum('ACTIVE', 'INACTIVE', 'SUSPENDED', 'FAILED', name='webhookstatus'), nullable=False, default='ACTIVE'),
        sa.Column('owner_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('org_id', sa.Integer(), sa.ForeignKey('orgs.id'), nullable=False),
        sa.Column('project_id', sa.Integer(), sa.ForeignKey('projects.id'), nullable=True),
        sa.Column('event_types', sa.JSON(), default=[]),
        sa.Column('headers', sa.JSON(), default={}),
        sa.Column('auth_type', sa.String(length=50), default='none'),
        sa.Column('auth_config', sa.JSON(), default={}),
        sa.Column('retry_count', sa.Integer(), default=3),
        sa.Column('timeout_seconds', sa.Integer(), default=30),
        sa.Column('is_active', sa.Boolean(), default=True),
        sa.Column('last_triggered_at', sa.DateTime(), nullable=True),
        sa.Column('last_success_at', sa.DateTime(), nullable=True),
        sa.Column('last_failure_at', sa.DateTime(), nullable=True),
        sa.Column('failure_count', sa.Integer(), default=0),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_webhooks_id', 'id'),
        sa.Index('ix_webhooks_owner_id', 'owner_id'),
        sa.Index('ix_webhooks_org_id', 'org_id'),
        sa.Index('ix_webhooks_status', 'status')
    )


def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table('webhooks')
    op.drop_table('team_invitations')
    op.drop_table('teams')
    op.drop_table('sessions')
    op.drop_table('permissions')
    op.drop_table('api_keys')