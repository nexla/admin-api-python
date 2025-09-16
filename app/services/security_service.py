import json
import ipaddress
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
import hashlib
import secrets
import logging

from ..models.security import (
    SecurityRole, RoleAssignment, SecurityPolicy, PolicyBinding,
    AccessControlEntry, SecurityAuditLog, SecurityRule, SecurityRuleViolation,
    ThreatIntelligence, SecurityIncident, DataClassification,
    ResourceType, ActionType, AccessDecision, RoleType
)
from ..models.user import User
from ..models.org import Org

logger = logging.getLogger(__name__)

class RBACService:
    """Role-Based Access Control Service"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_role(
        self,
        name: str,
        display_name: str,
        permissions: List[str],
        org_id: Optional[int] = None,
        role_type: RoleType = RoleType.CUSTOM,
        parent_role_id: Optional[int] = None,
        created_by: Optional[int] = None
    ) -> SecurityRole:
        """Create a new security role"""
        
        # Check if role name already exists
        existing_role = self.db.query(SecurityRole).filter(
            and_(
                SecurityRole.name == name,
                SecurityRole.org_id == org_id
            )
        ).first()
        
        if existing_role:
            raise ValueError(f"Role '{name}' already exists")
        
        # Calculate role level
        level = 0
        if parent_role_id:
            parent_role = self.db.query(SecurityRole).filter(
                SecurityRole.id == parent_role_id
            ).first()
            if parent_role:
                level = parent_role.level + 1
        
        role = SecurityRole(
            name=name,
            display_name=display_name,
            role_type=role_type,
            permissions=permissions,
            parent_role_id=parent_role_id,
            level=level,
            org_id=org_id,
            created_by=created_by
        )
        
        self.db.add(role)
        self.db.commit()
        self.db.refresh(role)
        
        return role
    
    def assign_role(
        self,
        user_id: int,
        role_id: int,
        resource_type: Optional[str] = None,
        resource_id: Optional[int] = None,
        expires_at: Optional[datetime] = None,
        conditions: Optional[Dict[str, Any]] = None,
        granted_by: Optional[int] = None
    ) -> RoleAssignment:
        """Assign a role to a user"""
        
        # Check if assignment already exists
        existing_assignment = self.db.query(RoleAssignment).filter(
            and_(
                RoleAssignment.user_id == user_id,
                RoleAssignment.role_id == role_id,
                RoleAssignment.resource_type == resource_type,
                RoleAssignment.resource_id == resource_id
            )
        ).first()
        
        if existing_assignment:
            raise ValueError("Role assignment already exists")
        
        assignment = RoleAssignment(
            user_id=user_id,
            role_id=role_id,
            resource_type=resource_type,
            resource_id=resource_id,
            expires_at=expires_at,
            conditions=conditions or {},
            granted_by=granted_by
        )
        
        self.db.add(assignment)
        self.db.commit()
        self.db.refresh(assignment)
        
        return assignment
    
    def check_permission(
        self,
        user_id: int,
        action: str,
        resource_type: str,
        resource_id: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check if user has permission to perform action on resource"""
        
        # Get user's active role assignments
        assignments = self._get_user_active_assignments(user_id)
        
        # Check each assignment
        for assignment in assignments:
            role = assignment.role
            
            # Check if role has the required permission
            if self._role_has_permission(role, action, resource_type, resource_id, context):
                # Log successful authorization
                self._log_access_event(
                    user_id, action, resource_type, resource_id, "allow", context
                )
                return True
        
        # Check explicit ACL entries
        if self._check_acl_permission(user_id, action, resource_type, resource_id, context):
            self._log_access_event(
                user_id, action, resource_type, resource_id, "allow", context
            )
            return True
        
        # Log denied access
        self._log_access_event(
            user_id, action, resource_type, resource_id, "deny", context
        )
        return False
    
    def _get_user_active_assignments(self, user_id: int) -> List[RoleAssignment]:
        """Get user's active role assignments"""
        now = datetime.utcnow()
        
        assignments = self.db.query(RoleAssignment).filter(
            and_(
                RoleAssignment.user_id == user_id,
                or_(
                    RoleAssignment.expires_at.is_(None),
                    RoleAssignment.expires_at > now
                )
            )
        ).all()
        
        return assignments
    
    def _role_has_permission(
        self,
        role: SecurityRole,
        action: str,
        resource_type: str,
        resource_id: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check if role has specific permission"""
        
        if not role.enabled:
            return False
        
        # Check direct permissions
        for permission in role.permissions:
            if self._permission_matches(permission, action, resource_type, resource_id):
                return True
        
        # Check inherited permissions from parent roles
        if role.parent_role:
            return self._role_has_permission(
                role.parent_role, action, resource_type, resource_id, context
            )
        
        return False
    
    def _permission_matches(
        self,
        permission: str,
        action: str,
        resource_type: str,
        resource_id: Optional[int] = None
    ) -> bool:
        """Check if permission string matches the required action and resource"""
        
        # Permission format: "action:resource_type:resource_id" or "action:resource_type:*"
        parts = permission.split(":")
        
        if len(parts) < 2:
            return False
        
        perm_action = parts[0]
        perm_resource = parts[1]
        perm_resource_id = parts[2] if len(parts) > 2 else "*"
        
        # Check action match (support wildcards)
        if perm_action != "*" and perm_action != action:
            return False
        
        # Check resource type match
        if perm_resource != "*" and perm_resource != resource_type:
            return False
        
        # Check resource ID match
        if perm_resource_id != "*":
            if resource_id is None:
                return False
            if perm_resource_id != str(resource_id):
                return False
        
        return True
    
    def _check_acl_permission(
        self,
        user_id: int,
        action: str,
        resource_type: str,
        resource_id: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check explicit ACL entries"""
        
        acl_entries = self.db.query(AccessControlEntry).filter(
            and_(
                AccessControlEntry.principal_type == "user",
                AccessControlEntry.principal_id == user_id,
                AccessControlEntry.resource_type == resource_type,
                AccessControlEntry.action == action,
                or_(
                    AccessControlEntry.resource_id.is_(None),
                    AccessControlEntry.resource_id == resource_id
                )
            )
        ).all()
        
        for entry in acl_entries:
            if entry.effect == AccessDecision.ALLOW:
                # Check conditions if any
                if self._evaluate_conditions(entry.conditions, context):
                    return True
            elif entry.effect == AccessDecision.DENY:
                # Explicit deny overrides allow
                if self._evaluate_conditions(entry.conditions, context):
                    return False
        
        return False
    
    def _evaluate_conditions(
        self,
        conditions: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Evaluate access conditions"""
        
        if not conditions:
            return True
        
        if not context:
            context = {}
        
        # Time-based conditions
        if "time_range" in conditions:
            time_range = conditions["time_range"]
            current_time = datetime.utcnow().time()
            
            start_time = datetime.strptime(time_range["start"], "%H:%M").time()
            end_time = datetime.strptime(time_range["end"], "%H:%M").time()
            
            if not (start_time <= current_time <= end_time):
                return False
        
        # IP-based conditions
        if "allowed_ips" in conditions and "client_ip" in context:
            allowed_ips = conditions["allowed_ips"]
            client_ip = context["client_ip"]
            
            if not self._ip_in_ranges(client_ip, allowed_ips):
                return False
        
        # Custom attribute conditions
        if "attributes" in conditions:
            for attr_name, attr_value in conditions["attributes"].items():
                if context.get(attr_name) != attr_value:
                    return False
        
        return True
    
    def _ip_in_ranges(self, ip: str, ranges: List[str]) -> bool:
        """Check if IP is in allowed ranges"""
        try:
            client_ip = ipaddress.ip_address(ip)
            
            for ip_range in ranges:
                if "/" in ip_range:
                    # CIDR notation
                    network = ipaddress.ip_network(ip_range, strict=False)
                    if client_ip in network:
                        return True
                else:
                    # Single IP
                    if client_ip == ipaddress.ip_address(ip_range):
                        return True
            
            return False
        except ValueError:
            return False
    
    def _log_access_event(
        self,
        user_id: int,
        action: str,
        resource_type: str,
        resource_id: Optional[int] = None,
        result: str = "allow",
        context: Optional[Dict[str, Any]] = None
    ):
        """Log access event for auditing"""
        
        audit_log = SecurityAuditLog(
            event_type="access_control",
            action=action,
            result=result,
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            details=context or {}
        )
        
        self.db.add(audit_log)
        self.db.commit()

class SecurityService:
    """Comprehensive security service"""
    
    def __init__(self, db: Session):
        self.db = db
        self.rbac = RBACService(db)
    
    def evaluate_security_rules(
        self,
        user_id: Optional[int],
        request_data: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """Evaluate security rules against request"""
        
        violations = []
        blocked = False
        
        # Get applicable security rules
        rules = self.db.query(SecurityRule).filter(
            SecurityRule.enabled == True
        ).all()
        
        for rule in rules:
            try:
                violation = self._evaluate_rule(rule, user_id, request_data)
                if violation:
                    violations.append(violation)
                    
                    # Check if this rule should block the request
                    if "block" in rule.actions:
                        blocked = True
                
            except Exception as e:
                logger.error(f"Error evaluating security rule {rule.id}: {e}")
        
        return not blocked, violations
    
    def _evaluate_rule(
        self,
        rule: SecurityRule,
        user_id: Optional[int],
        request_data: Dict[str, Any]
    ) -> Optional[str]:
        """Evaluate a single security rule"""
        
        conditions = rule.conditions
        
        # Rate limiting rules
        if rule.rule_type == "rate_limit":
            return self._evaluate_rate_limit_rule(rule, user_id, request_data)
        
        # IP whitelist rules
        elif rule.rule_type == "ip_whitelist":
            return self._evaluate_ip_whitelist_rule(rule, request_data)
        
        # Geo restriction rules
        elif rule.rule_type == "geo_restriction":
            return self._evaluate_geo_restriction_rule(rule, request_data)
        
        # Threat intelligence rules
        elif rule.rule_type == "threat_intelligence":
            return self._evaluate_threat_intelligence_rule(rule, request_data)
        
        return None
    
    def _evaluate_rate_limit_rule(
        self,
        rule: SecurityRule,
        user_id: Optional[int],
        request_data: Dict[str, Any]
    ) -> Optional[str]:
        """Evaluate rate limiting rule"""
        
        # Get rate limit parameters
        threshold = rule.threshold_value
        time_window = rule.time_window
        
        if not threshold or not time_window:
            return None
        
        # Count recent requests
        cutoff_time = datetime.utcnow() - timedelta(seconds=time_window)
        
        request_count = self.db.query(SecurityAuditLog).filter(
            and_(
                SecurityAuditLog.user_id == user_id,
                SecurityAuditLog.timestamp >= cutoff_time,
                SecurityAuditLog.event_type == "api_request"
            )
        ).count()
        
        if request_count >= threshold:
            # Record violation
            violation = SecurityRuleViolation(
                rule_id=rule.id,
                violation_type="rate_limit_exceeded",
                severity="high",
                description=f"Rate limit exceeded: {request_count}/{threshold} requests in {time_window}s",
                user_id=user_id,
                user_ip=request_data.get("client_ip"),
                action_taken="blocked" if "block" in rule.actions else "logged"
            )
            
            self.db.add(violation)
            self.db.commit()
            
            return f"Rate limit exceeded: {request_count}/{threshold}"
        
        return None
    
    def _evaluate_ip_whitelist_rule(
        self,
        rule: SecurityRule,
        request_data: Dict[str, Any]
    ) -> Optional[str]:
        """Evaluate IP whitelist rule"""
        
        client_ip = request_data.get("client_ip")
        if not client_ip:
            return None
        
        allowed_ips = rule.conditions.get("allowed_ips", [])
        
        if not self.rbac._ip_in_ranges(client_ip, allowed_ips):
            violation = SecurityRuleViolation(
                rule_id=rule.id,
                violation_type="ip_not_whitelisted",
                severity="high",
                description=f"IP {client_ip} not in whitelist",
                user_ip=client_ip,
                action_taken="blocked" if "block" in rule.actions else "logged"
            )
            
            self.db.add(violation)
            self.db.commit()
            
            return f"IP {client_ip} not whitelisted"
        
        return None
    
    def _evaluate_geo_restriction_rule(
        self,
        rule: SecurityRule,
        request_data: Dict[str, Any]
    ) -> Optional[str]:
        """Evaluate geo restriction rule"""
        
        # This would integrate with a GeoIP service
        # For now, return None (not implemented)
        return None
    
    def _evaluate_threat_intelligence_rule(
        self,
        rule: SecurityRule,
        request_data: Dict[str, Any]
    ) -> Optional[str]:
        """Evaluate threat intelligence rule"""
        
        client_ip = request_data.get("client_ip")
        if not client_ip:
            return None
        
        # Check if IP is in threat intelligence database
        threat = self.db.query(ThreatIntelligence).filter(
            and_(
                ThreatIntelligence.indicator_type == "ip",
                ThreatIntelligence.indicator_value == client_ip,
                ThreatIntelligence.active == True
            )
        ).first()
        
        if threat:
            violation = SecurityRuleViolation(
                rule_id=rule.id,
                violation_type="threat_intelligence_match",
                severity=threat.severity,
                description=f"IP {client_ip} matches threat intelligence: {threat.description}",
                user_ip=client_ip,
                action_taken="blocked" if "block" in rule.actions else "logged"
            )
            
            self.db.add(violation)
            self.db.commit()
            
            return f"Threat detected: {threat.description}"
        
        return None
    
    def create_security_incident(
        self,
        title: str,
        description: str,
        incident_type: str,
        severity: str,
        affected_users: List[int] = None,
        affected_resources: List[Dict[str, Any]] = None,
        org_id: Optional[int] = None,
        reported_by: Optional[int] = None
    ) -> SecurityIncident:
        """Create a security incident"""
        
        incident = SecurityIncident(
            title=title,
            description=description,
            incident_type=incident_type,
            severity=severity,
            affected_users=affected_users or [],
            affected_resources=affected_resources or [],
            org_id=org_id,
            reported_by=reported_by
        )
        
        self.db.add(incident)
        self.db.commit()
        self.db.refresh(incident)
        
        return incident
    
    def classify_data(
        self,
        data: Any,
        context: Dict[str, Any] = None
    ) -> Optional[DataClassification]:
        """Classify data based on content and context"""
        
        # Get active classification rules
        classifications = self.db.query(DataClassification).filter(
            DataClassification.active == True
        ).order_by(DataClassification.level.desc()).all()
        
        for classification in classifications:
            if self._data_matches_classification(data, classification, context):
                return classification
        
        return None
    
    def _data_matches_classification(
        self,
        data: Any,
        classification: DataClassification,
        context: Dict[str, Any] = None
    ) -> bool:
        """Check if data matches classification rules"""
        
        detection_rules = classification.detection_rules
        
        # Convert data to string for pattern matching
        data_str = str(data).lower() if data else ""
        
        for rule in detection_rules:
            rule_type = rule.get("type")
            
            if rule_type == "pattern":
                import re
                pattern = rule.get("pattern", "")
                if re.search(pattern, data_str, re.IGNORECASE):
                    return True
            
            elif rule_type == "keyword":
                keywords = rule.get("keywords", [])
                for keyword in keywords:
                    if keyword.lower() in data_str:
                        return True
            
            elif rule_type == "field_name":
                field_names = rule.get("field_names", [])
                if context and "field_name" in context:
                    if context["field_name"].lower() in [fn.lower() for fn in field_names]:
                        return True
        
        return False
    
    def generate_security_token(self, length: int = 32) -> str:
        """Generate a cryptographically secure random token"""
        return secrets.token_urlsafe(length)
    
    def hash_sensitive_data(self, data: str, salt: Optional[str] = None) -> Tuple[str, str]:
        """Hash sensitive data with salt"""
        if not salt:
            salt = secrets.token_hex(16)
        
        hash_obj = hashlib.pbkdf2_hmac('sha256', data.encode(), salt.encode(), 100000)
        return hash_obj.hex(), salt
    
    def audit_security_event(
        self,
        event_type: str,
        action: str,
        result: str,
        user_id: Optional[int] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
        request_data: Optional[Dict[str, Any]] = None
    ) -> SecurityAuditLog:
        """Audit a security event"""
        
        audit_log = SecurityAuditLog(
            event_type=event_type,
            action=action,
            result=result,
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details or {},
            user_ip=request_data.get("client_ip") if request_data else None,
            user_agent=request_data.get("user_agent") if request_data else None,
            request_id=request_data.get("request_id") if request_data else None,
            session_id=request_data.get("session_id") if request_data else None,
            method=request_data.get("method") if request_data else None,
            endpoint=request_data.get("endpoint") if request_data else None
        )
        
        self.db.add(audit_log)
        self.db.commit()
        self.db.refresh(audit_log)
        
        return audit_log