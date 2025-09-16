from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, Table
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from ..database import Base

# Association table for team-user many-to-many relationship
team_members = Table(
    'team_members',
    Base.metadata,
    Column('team_id', Integer, ForeignKey('teams.id')),
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('role', String(50), default='member'),  # member, admin, lead
    Column('joined_at', DateTime, default=func.now()),
    Column('added_by', Integer, ForeignKey('users.id'), nullable=True)
)

class Team(Base):
    __tablename__ = "teams"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    team_type = Column(String(50), default="project")  # project, department, security_group
    org_id = Column(Integer, ForeignKey('orgs.id'), nullable=False)
    
    # Team settings
    is_active = Column(Boolean, default=True)
    is_private = Column(Boolean, default=False)  # Private teams require invitation
    max_members = Column(Integer, nullable=True)  # Optional member limit
    
    # Team ownership
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    
    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    organization = relationship("Org", back_populates="teams")
    team_memberships = relationship("TeamMembership", back_populates="team")
    # members = relationship("User", secondary=team_members, primaryjoin="Team.id == team_members.c.team_id", secondaryjoin="User.id == team_members.c.user_id", back_populates="teams")  # Disabled temporarily
    created_by_user = relationship("User", foreign_keys=[created_by])
    owner = relationship("User", foreign_keys=[owner_id])
    flows = relationship("Flow", back_populates="team")
    
    @property
    def member_users(self) -> List:
        """Rails-style through relationship: Get users who are members of this team"""
        from .user import User
        return [membership.user for membership in self.team_memberships 
                if hasattr(membership, 'status') and membership.status == "ACTIVE"]
    
    def member_count(self) -> int:
        """Get the number of team members (Rails pattern)"""
        return len([m for m in self.team_memberships 
                   if hasattr(m, 'status') and m.status == "ACTIVE"])
    
    def is_member(self, user_id: int) -> bool:
        """Check if a user is a member of this team (Rails pattern)"""
        return any(m.user_id == user_id and 
                  (not hasattr(m, 'status') or m.status == "ACTIVE")
                  for m in self.team_memberships)
    
    def get_member_role(self, user_id: int) -> str:
        """Get the role of a user in this team"""
        if hasattr(self, '_admin_members') and any(m.id == user_id for m in self._admin_members):
            return "admin"
        elif self.is_member(user_id):
            return "member"
        return "none"
    
    # Rails business logic methods
    def tags_list(self) -> List[str]:
        """Get list of tag names (Rails pattern)"""
        # This would return tag names when tagging system is implemented
        return []
    
    def tag_list(self) -> List[str]:
        """Alias for tags_list (Rails pattern)"""
        return self.tags_list()
    
    @classmethod
    def build_from_input(cls, input_data: Dict[str, Any], user, org=None):
        """Build team from input data (Rails pattern)"""
        # Extract members before creating team
        members = input_data.pop('members', None)
        
        # Set owner and org
        input_data['owner'] = user
        input_data['org'] = org
        input_data['owner_id'] = user.id if user else None
        input_data['org_id'] = org.id if org else None
        input_data['created_by'] = user.id if user else None
        
        team = cls(**input_data)
        
        # Update members if provided
        if members:
            team.update_members(members, "reset")
        
        return team
    
    def org_member(self, org) -> bool:
        """Check if team belongs to org (Rails org_member? pattern)"""
        return self.org == org
    
    def orgs(self):
        """Get team's organization (Rails pattern)"""
        return self.org
    
    def update_mutable(self, request, user, input_data: Dict[str, Any]) -> None:
        """Update mutable fields (Rails update_mutable! pattern)"""
        if 'name' in input_data and input_data['name']:
            self.name = input_data['name']
        
        if 'description' in input_data:
            self.description = input_data['description']
        
        if 'members' in input_data:
            self.update_members(input_data['members'], "add")
    
    def update_members(self, members: List[Dict[str, Any]], mode: str) -> None:
        """Update team members (Rails pattern)"""
        if not members and mode != "reset":
            return
        
        members = members or []
        if not isinstance(members, list):
            members = [members]
        
        members_2 = []
        admins = []
        not_admins = []
        
        # Process each member
        for m in members:
            if not isinstance(m, dict):
                continue
            
            user = None
            if 'id' in m:
                # Find by ID - would need session to query
                user = self._find_user_by_id(m['id'])
            elif 'email' in m:
                # Find by email - would need session to query
                user = self._find_user_by_email(m['email'])
            
            if not user:
                raise ValueError(f"User invalid or not found: {m}")
            
            # Check if user is member of team's organization
            if self.org and not self._user_org_member(user, self.org):
                raise ValueError(f"User is not a member of team's organization: {m}")
            
            members_2.append(user)
            
            if m.get('admin'):
                admins.append(user)
            else:
                not_admins.append(user)
        
        # Remove duplicates
        members_2 = list({user.id: user for user in members_2}.values())
        
        if mode == "remove":
            self._remove_members(members_2)
            self.log_members_event("membership_removed", self, members, members_2)
            return
        
        if mode == "add":
            added_members = self._add_members(members_2)
            # Filter to only include actually added members
            filtered = [member for member in members if self._was_member_added(member, added_members)]
            self.log_members_event("membership_added", self, filtered, added_members)
        
        elif mode == "reset":
            self._reset_members(members_2)
            self.log_members_event("membership_updated", self, members, members_2)
        
        # Update admin roles
        self._update_admin_roles(admins, not_admins)
    
    def _find_user_by_id(self, user_id: int):
        """Find user by ID (helper method)"""
        from .user import User
        # This would query the User model when session is available
        # For now, return a mock user object
        return User(id=user_id, email=f"user{user_id}@example.com")
    
    def _find_user_by_email(self, email: str):
        """Find user by email (helper method)"""
        from .user import User
        # This would query the User model when session is available
        # For now, return a mock user object
        return User(id=999, email=email)
    
    def _user_org_member(self, user, org) -> bool:
        """Check if user is member of org (helper method)"""
        # This would check org membership when implemented
        if hasattr(user, 'org_member'):
            return user.org_member(org)
        return True  # Default to True for now
    
    def _remove_members(self, members_to_remove: List) -> None:
        """Remove members from team (helper method)"""
        # This would remove members from team_members association table
        print(f"DEBUG: Removing {len(members_to_remove)} members from team {self.id}")
        # Remove members from internal list
        if hasattr(self, '_members'):
            remove_ids = {m.id for m in members_to_remove}
            self._members = [m for m in self._members if m.id not in remove_ids]
    
    def _add_members(self, members_to_add: List) -> List:
        """Add members to team (helper method)"""
        # This would add members to team_members association table
        # and return the list of actually added members
        print(f"DEBUG: Adding {len(members_to_add)} members to team {self.id}")
        # Calculate which members were actually added (not already members)
        current_member_ids = [m.id for m in getattr(self, '_members', [])]
        actually_added = [m for m in members_to_add if m.id not in current_member_ids]
        
        # Update internal members list
        if not hasattr(self, '_members'):
            self._members = []
        for member in actually_added:
            if member not in self._members:
                self._members.append(member)
        
        return actually_added
    
    def _reset_members(self, new_members: List) -> None:
        """Reset team members (helper method)"""
        # This would replace all current members with new_members
        print(f"DEBUG: Resetting team {self.id} to {len(new_members)} members")
        # Store current members for logging
        self._previous_members = getattr(self, '_members', [])
        # Reset to new members
        self._members = new_members.copy()
    
    def _was_member_added(self, member_input: Dict[str, Any], added_members: List) -> bool:
        """Check if member was actually added (helper method)"""
        # This would check if the member from input was in the added_members list
        return True  # Simplified for now
    
    def _update_admin_roles(self, admins: List, not_admins: List) -> None:
        """Update admin roles for members (helper method)"""
        # This would update admin roles in team_members association table
        print(f"DEBUG: Setting {len(admins)} admins and {len(not_admins)} regular members for team {self.id}")
        # Store admin members for role checking
        self._admin_members = admins.copy()
        self._regular_members = not_admins.copy()
    
    def log_members_event(self, event_type: str, team, member_inputs: List, members: List) -> None:
        """Log membership events (Rails pattern)"""
        # This would log membership changes for audit trail
        print(f"DEBUG: Logging {event_type} for team {team.id}: {len(members)} members affected")
    
    def set_defaults(self, user, org) -> None:
        """Set default values (Rails pattern)"""
        self.owner = user
        self.organization = org
        self.created_by_user = user
        self.is_active = True
        self.is_private = False
        self.team_type = "project"
    
    # Rails access control methods (from AccessControls::Membership)
    def add_admin(self, accessor, expires_at=None):
        """Add admin access to a user or team (Rails pattern)"""
        if not hasattr(self, '_admin_access_controls'):
            self._admin_access_controls = []
        
        accessor_id = accessor.id if hasattr(accessor, 'id') else accessor
        if not any(ac['accessor_id'] == accessor_id for ac in self._admin_access_controls):
            self._admin_access_controls.append({
                'accessor_id': accessor_id,
                'accessor_type': accessor.__class__.__name__.upper() if hasattr(accessor, '__class__') else 'USER',
                'role': 'admin',
                'expires_at': expires_at
            })
        
        # Also add to admin members list for quick checking
        if not hasattr(self, '_admin_members'):
            self._admin_members = []
        if accessor not in self._admin_members:
            self._admin_members.append(accessor)
    
    def remove_admin(self, accessor, org=None):
        """Remove admin access from a user or team (Rails pattern)"""
        if not hasattr(self, '_admin_access_controls'):
            return
        
        accessor_id = accessor.id if hasattr(accessor, 'id') else accessor
        self._admin_access_controls = [
            ac for ac in self._admin_access_controls 
            if ac['accessor_id'] != accessor_id or ac['role'] != 'admin'
        ]
        
        # Remove from admin members list
        if hasattr(self, '_admin_members'):
            self._admin_members = [m for m in self._admin_members if m != accessor]
    
    def has_admin_access(self, accessor):
        """Check if accessor has admin access (Rails pattern)"""
        # Owner always has admin access
        if hasattr(accessor, 'id') and accessor.id == self.owner_id:
            return True
        
        # Check stored admin access controls
        if hasattr(self, '_admin_access_controls'):
            accessor_id = accessor.id if hasattr(accessor, 'id') else accessor
            for ac in self._admin_access_controls:
                if ac['accessor_id'] == accessor_id and ac['role'] == 'admin':
                    if ac.get('expires_at') is None or ac['expires_at'] > datetime.now():
                        return True
        
        return False
    
    def delete_acl_entries(self):
        """Delete access control entries (Rails pattern)"""
        # This would delete all access control entries for this team
        if hasattr(self, '_admin_access_controls'):
            self._admin_access_controls.clear()
        if hasattr(self, '_admin_members'):
            self._admin_members.clear()
        print(f"DEBUG: Deleted ACL entries for team {self.id}")
    
    def projects(self, access_role='all', org_ignored=None):
        """Get accessible projects (Rails pattern)"""
        from .project import Project
        # This would implement Project.accessible(self, access_role, self.org)
        # For now, return empty list as placeholder
        print(f"DEBUG: Getting projects for team {self.id} with access_role {access_role}")
        return []
    
    def destroy(self):
        """Destroy team with proper cleanup (Rails pattern)"""
        # This would be wrapped in a database transaction
        try:
            # Delete access control entries first
            self.delete_acl_entries()
            
            # Clear all relationships
            if hasattr(self, '_members'):
                self._members.clear()
            if hasattr(self, '_admin_members'):
                self._admin_members.clear()
            
            # Mark as inactive
            self.is_active = False
            
            print(f"DEBUG: Team {self.id} destroyed successfully")
            return True
        except Exception as e:
            print(f"DEBUG: Error destroying team {self.id}: {e}")
            return False
    
    # Rails predicate methods
    def active(self) -> bool:
        """Check if team is active (Rails pattern)"""
        return self.is_active
    
    def private(self) -> bool:
        """Check if team is private (Rails pattern)"""
        return self.is_private
    
    def public(self) -> bool:
        """Check if team is public (Rails pattern)"""
        return not self.is_private
    
    def full(self) -> bool:
        """Check if team is at capacity (Rails pattern)"""
        if self.max_members is None:
            return False
        return self.member_count() >= self.max_members
    
    def can_add_members(self) -> bool:
        """Check if team can accept new members (Rails pattern)"""
        return self.active() and not self.full()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert team to dictionary for API responses"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'team_type': self.team_type,
            'is_active': self.is_active,
            'is_private': self.is_private,
            'max_members': self.max_members,
            'member_count': self.member_count(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'created_by': self.created_by,
            'tags': self.tags_list(),
            'active': self.active(),
            'private': self.private(),
            'public': self.public(),
            'full': self.full(),
            'can_add_members': self.can_add_members()
        }

class TeamInvitation(Base):
    __tablename__ = "team_invitations"
    
    id = Column(Integer, primary_key=True, index=True)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    invited_user_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    invited_email = Column(String(254), nullable=True)  # For external invitations
    role = Column(String(50), default='member')
    
    # Invitation metadata
    invited_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    invitation_message = Column(Text)
    invitation_token = Column(String(255), unique=True, nullable=False)
    
    # Status tracking
    status = Column(String(50), default='pending')  # pending, accepted, declined, expired
    expires_at = Column(DateTime, nullable=False)
    responded_at = Column(DateTime)
    
    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    team = relationship("Team")
    invited_user = relationship("User", foreign_keys=[invited_user_id])
    inviter = relationship("User", foreign_keys=[invited_by])