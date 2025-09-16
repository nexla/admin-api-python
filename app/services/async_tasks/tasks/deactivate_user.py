"""
Deactivate User Task - Handle user deactivation and resource management.
Deactivates users and pauses their associated resources.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_

from ....database import SessionLocal
from ....models.user import User
from ....models.org import Org
from ....models.org_membership import OrgMembership  
from ....models.data_source import DataSource
from ....models.enums import UserStatus, DataSourceStatus
from fastapi import HTTPException
from ...audit_service import AuditService, AuditActions
from ..manager import BaseAsyncTask

logger = logging.getLogger(__name__)


class DeactivateUserTask(BaseAsyncTask):
    """Task for deactivating users and managing their resources"""
    
    def check_preconditions(self):
        """Check if the task owner has permission to deactivate the target user"""
        try:
            user_id = self.arguments.get('user_id')
            if not user_id:
                raise HTTPException(status_code=400, detail="User ID is required")
            
            # Get target user
            target_user = self.db.query(User).filter(User.id == user_id).first()
            if not target_user:
                raise HTTPException(status_code=404, detail="Target user not found")
            
            # Check if task owner has admin permissions over target user
            # This means they must be an admin in a shared organization
            owner_admin_orgs = self.db.query(OrgMembership.org_id).filter(
                and_(
                    OrgMembership.user_id == self.task.owner_id,
                    OrgMembership.role == 'admin'
                )
            ).subquery()
            
            target_user_orgs = self.db.query(OrgMembership.org_id).filter(
                OrgMembership.user_id == user_id
            ).subquery()
            
            shared_admin_access = self.db.query(owner_admin_orgs).filter(
                owner_admin_orgs.c.org_id.in_(target_user_orgs)
            ).first()
            
            if not shared_admin_access:
                raise HTTPException(
                    status_code=403,
                    detail="You do not have admin access to deactivate this user"
                )
            
            # Check if target user is already deactivated
            if target_user.status == UserStatus.DEACTIVATED:
                raise HTTPException(status_code=400, detail="User is already deactivated")
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Precondition check failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to validate user deactivation request")
    
    def perform(self):
        """Deactivate the user and pause their resources"""
        try:
            user_id = self.arguments.get('user_id')
            
            self.update_progress(10, "Loading user and resources")
            
            # Get target user
            target_user = self.db.query(User).filter(User.id == user_id).first()
            if not target_user:
                raise Exception("Target user not found")
            
            # Get all data sources owned by the user
            user_data_sources = self.db.query(DataSource).filter(
                and_(
                    DataSource.owner_id == user_id,
                    DataSource.status == DataSourceStatus.ACTIVE
                )
            ).all()
            
            self.update_progress(30, f"Found {len(user_data_sources)} active data sources")
            
            # Pause all user's active data sources
            paused_sources = []
            for data_source in user_data_sources:
                try:
                    old_status = data_source.status
                    data_source.status = DataSourceStatus.PAUSED
                    data_source.updated_at = datetime.utcnow()
                    
                    # Log the status change
                    AuditService.log_action(
                        db=self.db,
                        user_id=self.task.owner_id,
                        action=AuditActions.UPDATE,
                        resource_type='DataSource',
                        resource_id=data_source.id,
                        resource_name=data_source.name,
                        old_values={'status': old_status.value},
                        new_values={'status': DataSourceStatus.PAUSED.value},
                        details={
                            'reason': 'user_deactivation',
                            'target_user_id': user_id,
                            'deactivated_by': self.task.owner_id
                        },
                        org_id=data_source.org_id,
                        risk_level="medium"
                    )
                    
                    paused_sources.append(data_source.id)
                    
                except Exception as e:
                    logger.error(f"Failed to pause data source {data_source.id}: {str(e)}")
                    continue
            
            self.update_progress(60, f"Paused {len(paused_sources)} data sources")
            
            # Deactivate the user
            old_status = target_user.status
            target_user.status = UserStatus.DEACTIVATED
            target_user.deactivated_at = datetime.utcnow()
            target_user.updated_at = datetime.utcnow()
            
            # Set up audit metadata from task request data
            request_data = self.task.request_data or {}
            
            # Log user deactivation
            AuditService.log_action(
                db=self.db,
                user_id=self.task.owner_id,
                action=AuditActions.DEACTIVATE,
                resource_type='User',
                resource_id=target_user.id,
                resource_name=target_user.email,
                old_values={'status': old_status.value},
                new_values={'status': UserStatus.DEACTIVATED.value},
                details={
                    'paused_data_sources': paused_sources,
                    'deactivation_reason': 'admin_action',
                    'request_metadata': {
                        'host': request_data.get('host'),
                        'ip': request_data.get('request_ip'),
                        'url': request_data.get('request_url'),
                        'user_agent': request_data.get('request_user_agent')
                    }
                },
                org_id=self.task.org_id,
                risk_level="high"
            )
            
            # Create version record for audit trail (if versioning is enabled)
            if hasattr(target_user, 'create_version'):
                version_metadata = {
                    'request_ip': request_data.get('request_ip'),
                    'request_url': request_data.get('request_url'),
                    'request_user_agent': request_data.get('request_user_agent'),
                    'host': request_data.get('host'),
                    'user_id': self.task.owner_id,
                    'user_email': self.db.query(User.email).filter(User.id == self.task.owner_id).scalar(),
                    'org_id': self.task.org_id
                }
                target_user.create_version(metadata=version_metadata)
            
            self.update_progress(90, "Saving changes")
            
            # Commit all changes
            self.db.commit()
            
            self.update_progress(100, "User deactivation completed")
            
            # Set task result
            result = {
                'user_id': target_user.id,
                'user_email': target_user.email,
                'deactivated_at': target_user.deactivated_at.isoformat(),
                'paused_data_sources': paused_sources,
                'paused_sources_count': len(paused_sources),
                'deactivated_by': self.task.owner_id
            }
            
            self.set_result(result)
            
            logger.info(
                f"Successfully deactivated user {target_user.id} and paused "
                f"{len(paused_sources)} data sources"
            )
            
        except Exception as e:
            logger.error(f"Failed to deactivate user: {str(e)}")
            self.db.rollback()
            raise