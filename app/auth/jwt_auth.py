"""
JWT Authentication service - Python equivalent of Rails JWT authentication.
Handles token generation, validation, and user authentication.
"""

import jwt
from jwt.exceptions import DecodeError, ExpiredSignatureError
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from passlib.context import CryptContext
import logging

from ..config import settings
from ..database import get_db
from ..models.user import User

logger = logging.getLogger(__name__)

# JWT Configuration
ALGORITHM = settings.ALGORITHM
SECRET_KEY = settings.SECRET_KEY
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_TOKEN_EXPIRE_DAYS = settings.REFRESH_TOKEN_EXPIRE_DAYS

# HTTP Bearer token scheme
security = HTTPBearer(auto_error=False)

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# Password utilities
class PasswordUtils:
    """Password hashing and verification utilities"""
    
    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password for storing"""
        return pwd_context.hash(password)
    
    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        return pwd_context.verify(plain_password, hashed_password)


class JWTAuth:
    """JWT Authentication service class"""
    
    @staticmethod
    def create_access_token(user: User, expires_delta: Optional[timedelta] = None) -> str:
        """Create JWT access token for user"""
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        # Token payload (similar to Rails JWT payload)
        payload = {
            "user_id": user.id,
            "email": user.email,
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        }
        
        # Add organization context if available
        if user.default_org_id:
            payload["default_org_id"] = user.default_org_id
        
        token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
        logger.info(f"Access token created for user: {user.email}")
        return token
    
    @staticmethod
    def create_refresh_token(user: User) -> str:
        """Create JWT refresh token for user"""
        expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        
        payload = {
            "user_id": user.id,
            "email": user.email,
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "refresh"
        }
        
        token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
        logger.info(f"Refresh token created for user: {user.email}")
        return token
    
    @staticmethod
    def verify_token(token: str) -> Dict[str, Any]:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            return payload
        except ExpiredSignatureError:
            logger.warning("Token has expired")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired",
                headers={"WWW-Authenticate": "Bearer"},
            )
        except DecodeError as e:
            logger.warning(f"Invalid token: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )
    
    @staticmethod
    def get_user_from_token(token: str, db: Session) -> User:
        """Get user from JWT token"""
        payload = JWTAuth.verify_token(token)
        user_id = payload.get("user_id")
        
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload"
            )
        
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found"
            )
        
        # Check if user is still active
        if user.status != "ACTIVE":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User account is not active"
            )
        
        return user
    
    @staticmethod
    def refresh_access_token(refresh_token: str, db: Session) -> str:
        """Generate new access token from refresh token"""
        payload = JWTAuth.verify_token(refresh_token)
        
        # Verify it's a refresh token
        if payload.get("type") != "refresh":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token type"
            )
        
        user = JWTAuth.get_user_from_token(refresh_token, db)
        return JWTAuth.create_access_token(user)


class AuthService:
    """High-level authentication service"""
    
    @staticmethod
    def authenticate_user(email: str, password: str, db: Session) -> User:
        """Authenticate user with email and password"""
        # Find user by email (case insensitive)
        user = db.query(User).filter(func.lower(User.email) == func.lower(email)).first()
        if not user:
            logger.warning(f"Authentication attempt for non-existent user: {email}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password"
            )
        
        # Verify password using bcrypt
        if not user.password_digest or not PasswordUtils.verify_password(password, user.password_digest):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password"
            )
        
        # Check if account is active
        if user.status != "ACTIVE":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Account is not active"
            )
        
        # Save successful authentication
        db.commit()
        return user
    
    @staticmethod
    def login(email: str, password: str, db: Session) -> Dict[str, str]:
        """Complete login process - authenticate and return tokens"""
        user = AuthService.authenticate_user(email, password, db)
        
        # Generate tokens
        access_token = JWTAuth.create_access_token(user)
        refresh_token = JWTAuth.create_refresh_token(user)
        
        # TODO: Create user login audit record
        # user_login_audit = UserLoginAudit(
        #     user_id=user.id,
        #     login_at=datetime.utcnow(),
        #     ip_address=request.client.host,  # Would need to be passed in
        #     user_agent=request.headers.get("user-agent")
        # )
        # db.add(user_login_audit)
        # db.commit()
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "user": user.to_dict()
        }
    
    @staticmethod
    def create_user(email: str, password: str, full_name: str, db: Session) -> User:
        """Create new user account"""
        # Check if user already exists
        existing_user = db.query(User).filter(func.lower(User.email) == func.lower(email)).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this email already exists"
            )
        
        # Create new user
        now = datetime.utcnow()
        hashed_password = PasswordUtils.hash_password(password)
        user = User(
            email=email,
            full_name=full_name,
            status="ACTIVE",  # Using valid enum value from database
            password_digest=hashed_password,
            created_at=now,
            updated_at=now
        )
        
        db.add(user)
        db.commit()
        db.refresh(user)
        
        logger.info(f"New user created: {email}")
        return user


# FastAPI Dependencies
async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """FastAPI dependency to get current authenticated user"""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return JWTAuth.get_user_from_token(credentials.credentials, db)


async def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """FastAPI dependency to get current active user"""
    if current_user.status != "ACTIVE":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    return current_user


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db)
) -> Optional[User]:
    """FastAPI dependency to optionally get current user (doesn't raise if not authenticated)"""
    if not credentials:
        return None
    
    try:
        return JWTAuth.get_user_from_token(credentials.credentials, db)
    except HTTPException:
        return None


# Authentication utilities
def require_admin(user: User, org_id: Optional[int] = None):
    """Utility to check if user has admin role"""
    if org_id and not user.is_org_admin(org_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required"
        )
    # Global admin check would go here
    
def require_active_user(user: User):
    """Utility to ensure user is active"""
    if user.status != "ACTIVE":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Active user account required"
        )

def require_email_verified(user: User):
    """Utility to ensure user has verified email"""
    if not user.email_verified_at:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Email verification required"
        )

def require_tos_signed(user: User):
    """Utility to ensure user has signed ToS"""
    if not user.tos_signed_at:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Terms of service must be signed"
        )