from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List, Union
import json

from app.database import Base

class CustodianModes(PyEnum):
    ADD = "add"
    REMOVE = "remove"
    RESET = "reset"

class Domain(Base):
    __tablename__ = 'domains'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    parent_id = Column(Integer, ForeignKey('domains.id'))
    
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id], back_populates="domains")
    org = relationship("Org", foreign_keys=[org_id], back_populates="domains")
    parent = relationship("Domain", remote_side=[id], back_populates="children")
    children = relationship("Domain", back_populates="parent")
    
    # Many-to-many relationships
    marketplace_items = relationship("MarketplaceItem", secondary="domain_marketplace_items", back_populates="domains")
    domain_custodian_users = relationship("User", secondary="domain_custodians", back_populates="custodian_domains")
    
    # Other relationships
    domain_custodians = relationship("DomainCustodian", back_populates="domain")
    approval_requests = relationship("ApprovalRequest", back_populates="domain")
    
    @classmethod
    def build_from_input(cls, input_data: Dict[str, Any], user, org) -> 'Domain':
        """Factory method to create domain from input data"""
        domain = cls()
        
        # Set default org_id if not provided
        if 'org_id' not in input_data:
            input_data['org_id'] = org.id
        
        domain.update_mutable_(input_data, user, org)
        return domain
    
    def update_mutable_(self, input_data: Dict[str, Any], user, org) -> None:
        """Update mutable attributes"""
        if 'name' in input_data:
            self.name = input_data['name']
        if 'description' in input_data:
            self.description = input_data['description']
        if 'parent_id' in input_data:
            self.parent_id = input_data['parent_id']
        
        # Update ownership
        if user.id != self.owner_id:
            self.owner_id = user.id
        if org.id != self.org_id:
            self.org_id = org.id
        
        # Validation
        if not self.name or not self.org_id:
            raise ValueError("Domain name and org_id are required")
        
        # Permission check (simplified)
        if not user.can_manage_resource_(self):
            raise ValueError("Domain owner doesn't have permission to edit domains for this org")
        
        # Update custodians if provided
        if 'custodians' in input_data:
            self.update_custodians_(user, input_data['custodians'], CustodianModes.RESET)
    
    def update_custodians_(self, api_user, custodians: List[Dict[str, Any]], mode: CustodianModes) -> List:
        """Update domain custodians"""
        from app.models.user import User
        from app.models.domain_custodian import DomainCustodian
        
        custodians = custodians or []
        
        # Find users from custodian data
        users = []
        for custodian in custodians:
            user = None
            if custodian.get('email'):
                user = User.query.filter_by(email=custodian['email']).first()
            elif custodian.get('id'):
                user = User.query.get(custodian['id'])
            
            if not user:
                raise ValueError(f"Cannot find user with email or id {custodian.get('email') or custodian.get('id')}")
            
            if mode != CustodianModes.REMOVE and not user.org_member_(self.org):
                raise ValueError("Only domain's org members should be assigned as custodians")
            
            users.append(user)
        
        # Update custodians based on mode
        if mode == CustodianModes.ADD:
            for user in users:
                existing = DomainCustodian.query.filter_by(
                    domain_id=self.id, 
                    user_id=user.id
                ).first()
                if not existing:
                    custodian = DomainCustodian(
                        domain_id=self.id,
                        user_id=user.id,
                        org_id=self.org_id
                    )
                    # In production: db.session.add(custodian)
        
        elif mode == CustodianModes.REMOVE:
            for user in users:
                DomainCustodian.query.filter_by(
                    domain_id=self.id,
                    user_id=user.id
                ).delete()
        
        elif mode == CustodianModes.RESET:
            # Remove all existing custodians
            DomainCustodian.query.filter_by(domain_id=self.id).delete()
            
            # Add new custodians
            for user in users:
                custodian = DomainCustodian(
                    domain_id=self.id,
                    user_id=user.id,
                    org_id=self.org_id
                )
                # In production: db.session.add(custodian)
        
        return self.domain_custodian_users
    
    def items_count(self) -> int:
        """Get count of active marketplace items"""
        # In production, this would use the SQL query from Rails model
        active_items = [item for item in self.marketplace_items if item.active_()]
        return len(active_items)
    
    def active_custodian_user_(self, user) -> bool:
        """Check if user is an active custodian"""
        # Check domain custodians
        domain_custodians = [dc for dc in self.domain_custodians if dc.active_()]
        domain_custodian_user_ids = [dc.user_id for dc in domain_custodians]
        
        if user.id in domain_custodian_user_ids:
            return True
        
        # Check org custodians
        org_custodians = [oc for oc in self.org.org_custodians if oc.active_()]
        org_custodian_user_ids = [oc.user_id for oc in org_custodians]
        
        if user.id in org_custodian_user_ids:
            return True
        
        # Check org admin access
        return self.org.has_admin_access_(user)
    
    def requested_marketplace_items_ids(self) -> List[int]:
        """Get IDs of marketplace items with pending approval requests"""
        from app.models.approval_request import ApprovalRequest
        from app.models.approval_step import ApprovalStep
        
        # Get pending approval requests for this domain
        pending_requests = [ar for ar in self.approval_requests if ar.pending_()]
        approval_request_ids = [ar.id for ar in pending_requests]
        
        if not approval_request_ids:
            return []
        
        # Get data set IDs from FillBasics approval steps
        steps = ApprovalStep.query.filter(
            ApprovalStep.approval_request_id.in_(approval_request_ids),
            ApprovalStep.step_name == 'FillBasics'
        ).all()
        
        data_set_ids = []
        for step in steps:
            if step.result and isinstance(step.result, dict):
                data_set_id = step.result.get('data_set_id')
                if data_set_id:
                    data_set_ids.append(data_set_id)
        
        return data_set_ids
    
    # Predicate methods (Rails pattern)
    def has_parent_(self) -> bool:
        """Check if domain has a parent"""
        return self.parent_id is not None
    
    def root_domain_(self) -> bool:
        """Check if this is a root domain (no parent)"""
        return not self.has_parent_()
    
    def has_children_(self) -> bool:
        """Check if domain has children"""
        return len(self.children) > 0
    
    def leaf_domain_(self) -> bool:
        """Check if this is a leaf domain (no children)"""
        return not self.has_children_()
    
    def has_marketplace_items_(self) -> bool:
        """Check if domain has marketplace items"""
        return len(self.marketplace_items) > 0
    
    def has_active_marketplace_items_(self) -> bool:
        """Check if domain has active marketplace items"""
        return self.items_count() > 0
    
    def has_custodians_(self) -> bool:
        """Check if domain has custodians"""
        return len(self.domain_custodians) > 0
    
    def has_active_custodians_(self) -> bool:
        """Check if domain has active custodians"""
        active_custodians = [dc for dc in self.domain_custodians if dc.active_()]
        return len(active_custodians) > 0
    
    def has_pending_requests_(self) -> bool:
        """Check if domain has pending approval requests"""
        return len(self.requested_marketplace_items_ids()) > 0
    
    def managed_by_user_(self, user) -> bool:
        """Check if domain is managed by user (owner or custodian)"""
        if self.owner_id == user.id:
            return True
        return self.active_custodian_user_(user)
    
    def accessible_by_user_(self, user) -> bool:
        """Check if domain is accessible by user"""
        # Check if user is owner, custodian, or org admin
        return (self.owner_id == user.id or 
                self.active_custodian_user_(user) or
                self.org.has_admin_access_(user))
    
    # Hierarchy methods
    def depth(self) -> int:
        """Get domain depth in hierarchy"""
        if not self.has_parent_():
            return 0
        return 1 + self.parent.depth()
    
    def root(self) -> 'Domain':
        """Get root domain"""
        if self.root_domain_():
            return self
        return self.parent.root()
    
    def ancestors(self) -> List['Domain']:
        """Get all ancestor domains"""
        if self.root_domain_():
            return []
        
        ancestors = [self.parent]
        ancestors.extend(self.parent.ancestors())
        return ancestors
    
    def descendants(self) -> List['Domain']:
        """Get all descendant domains"""
        descendants = list(self.children)
        for child in self.children:
            descendants.extend(child.descendants())
        return descendants
    
    def siblings(self) -> List['Domain']:
        """Get sibling domains"""
        if self.root_domain_():
            # Root domains are siblings of other root domains in same org
            return Domain.query.filter(
                Domain.org_id == self.org_id,
                Domain.parent_id.is_(None),
                Domain.id != self.id
            ).all()
        
        return [child for child in self.parent.children if child.id != self.id]
    
    def full_path(self) -> str:
        """Get full hierarchical path"""
        if self.root_domain_():
            return self.name
        return f"{self.parent.full_path()} / {self.name}"
    
    # Custodian management methods
    def add_custodian(self, user) -> None:
        """Add a custodian to the domain"""
        self.update_custodians_(user, [{'id': user.id}], CustodianModes.ADD)
    
    def remove_custodian(self, user) -> None:
        """Remove a custodian from the domain"""
        self.update_custodians_(user, [{'id': user.id}], CustodianModes.REMOVE)
    
    def custodian_count(self) -> int:
        """Get count of domain custodians"""
        return len(self.domain_custodians)
    
    def active_custodian_count(self) -> int:
        """Get count of active domain custodians"""
        active_custodians = [dc for dc in self.domain_custodians if dc.active_()]
        return len(active_custodians)
    
    # Marketplace item methods
    def marketplace_item_count(self) -> int:
        """Get total marketplace item count"""
        return len(self.marketplace_items)
    
    def active_marketplace_item_count(self) -> int:
        """Get active marketplace item count"""
        return self.items_count()
    
    def data_set_count(self) -> int:
        """Get count of data sets through marketplace items"""
        data_sets = []
        for item in self.marketplace_items:
            if hasattr(item, 'data_sets'):
                data_sets.extend(item.data_sets)
        return len(set(data_sets))  # Remove duplicates
    
    # Display methods
    def hierarchy_display(self) -> str:
        """Get hierarchical display with indentation"""
        indent = "  " * self.depth()
        return f"{indent}{self.name}"
    
    def status_display(self) -> str:
        """Get status display based on marketplace items and custodians"""
        if self.has_active_marketplace_items_():
            return "Active"
        elif self.has_marketplace_items_():
            return "Inactive"
        else:
            return "Empty"
    
    def custodian_summary(self) -> str:
        """Get custodian summary"""
        active_count = self.active_custodian_count()
        total_count = self.custodian_count()
        
        if total_count == 0:
            return "No custodians"
        elif active_count == total_count:
            return f"{total_count} custodians"
        else:
            return f"{active_count}/{total_count} active custodians"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'parent_id': self.parent_id,
            'name': self.name,
            'description': self.description,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            
            # Counts
            'items_count': self.items_count(),
            'marketplace_item_count': self.marketplace_item_count(),
            'active_marketplace_item_count': self.active_marketplace_item_count(),
            'data_set_count': self.data_set_count(),
            'custodian_count': self.custodian_count(),
            'active_custodian_count': self.active_custodian_count(),
            'children_count': len(self.children),
            
            # Hierarchy
            'depth': self.depth(),
            'full_path': self.full_path(),
            
            # Predicate methods
            'has_parent': self.has_parent_(),
            'root_domain': self.root_domain_(),
            'has_children': self.has_children_(),
            'leaf_domain': self.leaf_domain_(),
            'has_marketplace_items': self.has_marketplace_items_(),
            'has_active_marketplace_items': self.has_active_marketplace_items_(),
            'has_custodians': self.has_custodians_(),
            'has_active_custodians': self.has_active_custodians_(),
            'has_pending_requests': self.has_pending_requests_(),
            
            # Display values
            'hierarchy_display': self.hierarchy_display(),
            'status_display': self.status_display(),
            'custodian_summary': self.custodian_summary()
        }