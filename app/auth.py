from datetime import datetime, timedelta
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from .config import settings
from .database import get_db
from .models.user import User

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT token scheme
security = HTTPBearer()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Hash a password."""
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None, user: Optional[User] = None):
    """Create a JWT access token with password signature for invalidation (Rails pattern)."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    # Add password signature for token invalidation (Rails pattern)
    if user:
        to_encode.update({"password_signature": user.password_signature()})
    
    to_encode.update({"exp": expire, "iat": datetime.utcnow()})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> Optional[dict]:
    """Verify and decode a JWT token."""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload
    except JWTError:
        return None

def authenticate_user(db: Session, email: str, password: str) -> Optional[User]:
    """Authenticate a user by email and password with Rails-style security checks."""
    user = db.query(User).filter(User.email == email).first()
    if not user:
        return None
        
    # Check if account is locked (Rails pattern)
    if user.account_locked_():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is locked due to too many failed login attempts"
        )
    
    # Verify password
    if not verify_password(password, user.password_digest):
        # Increment retry count and potentially lock account (Rails pattern)
        user.increment_password_retry_count()
        db.commit()
        return None
    
    # Reset password retry count on successful login (Rails pattern)
    if user.password_retry_count > 0:
        user.reset_password_retry_count()
        db.commit()
    
    return user

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """Get the current authenticated user from JWT token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = verify_token(credentials.credentials)
        if payload is None:
            raise credentials_exception
        
        user_id: int = payload.get("user_id")
        if user_id is None:
            raise credentials_exception
            
    except JWTError:
        raise credentials_exception
    
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise credentials_exception
    
    # Validate password signature for token invalidation (Rails pattern)
    token_password_signature = payload.get("password_signature")
    if token_password_signature and token_password_signature != user.password_signature():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalidated due to password change"
        )
        
    # Check if user is active
    if not user.active_():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Inactive user"
        )
    
    # Check if account is locked (Rails pattern)
    if user.account_locked_():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is locked"
        )
    
    # Check if password change is required (Rails pattern)
    if user.password_change_required_():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Password change required"
        )
        
    return user

def get_current_active_user(current_user: User = Depends(get_current_user)) -> User:
    """Get current active user (Rails pattern)."""
    if not current_user.active_():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    return current_user

def create_refresh_token(user: User) -> str:
    """Create a refresh token for JWT token renewal (Rails pattern)."""
    data = {
        "user_id": user.id,
        "type": "refresh",
        "password_signature": user.password_signature()
    }
    # Refresh tokens have longer expiration (7 days default)
    expires_delta = timedelta(days=7)
    return create_access_token(data, expires_delta, user)

def refresh_access_token(refresh_token: str, db: Session) -> Optional[dict]:
    """Refresh access token using refresh token (Rails pattern)."""
    try:
        payload = verify_token(refresh_token)
        if payload is None or payload.get("type") != "refresh":
            return None
            
        user_id = payload.get("user_id")
        if not user_id:
            return None
            
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return None
            
        # Validate password signature
        token_password_signature = payload.get("password_signature")
        if token_password_signature and token_password_signature != user.password_signature():
            return None
            
        # Check if user is still active and not locked
        if not user.active_() or user.account_locked_():
            return None
            
        # Create new access token
        access_token_data = {"user_id": user.id, "type": "access"}
        new_access_token = create_access_token(access_token_data, user=user)
        
        return {
            "access_token": new_access_token,
            "token_type": "bearer",
            "user_id": user.id
        }
        
    except JWTError:
        return None

def require_super_user(current_user: User = Depends(get_current_user)) -> User:
    """Require super user privileges."""
    if not hasattr(current_user, 'super_user') or not current_user.super_user():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough privileges"
        )
    return current_user