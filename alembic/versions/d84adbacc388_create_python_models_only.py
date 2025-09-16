"""Create Python models only

Revision ID: d84adbacc388
Revises: 6cd641b58ca2
Create Date: 2025-09-09 17:45:27.123456

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'd84adbacc388'
down_revision = '6cd641b58ca2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create core tables for Python FastAPI models
    
    # Create clusters table
    op.create_table('clusters',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=True),
        sa.Column('config', sa.JSON(), nullable=True),
        sa.Column('is_default', sa.Boolean(), nullable=True),
        sa.Column('endpoint_url', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_clusters_id', 'id')
    )
    
    # Create vendors table
    op.create_table('vendors',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('display_name', sa.String(length=255), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('config', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_vendors_id', 'id'),
        sa.UniqueConstraint('name')
    )
    
    # Create rate_limits table
    op.create_table('rate_limits',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('requests_per_minute', sa.Integer(), nullable=True),
        sa.Column('requests_per_hour', sa.Integer(), nullable=True),
        sa.Column('requests_per_day', sa.Integer(), nullable=True),
        sa.Column('bytes_per_minute', sa.BigInteger(), nullable=True),
        sa.Column('bytes_per_hour', sa.BigInteger(), nullable=True),
        sa.Column('bytes_per_day', sa.BigInteger(), nullable=True),
        sa.Column('resource_type', sa.String(length=50), nullable=True),
        sa.Column('resource_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.Index('ix_rate_limits_id', 'id')
    )


def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table('rate_limits')
    op.drop_table('vendors')
    op.drop_table('clusters')