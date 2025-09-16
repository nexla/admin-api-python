from typing import Optional, Dict, Any
import os
import base64
import secrets
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import hashlib
import json


class EncryptionService:
    """Service for encrypting and decrypting sensitive data"""
    
    def __init__(self):
        self._key = self._get_encryption_key()
        self._fernet = Fernet(self._key)
    
    def _get_encryption_key(self) -> bytes:
        """Get encryption key from environment or generate one"""
        key_str = os.environ.get('ENCRYPTION_KEY')
        if key_str:
            return base64.urlsafe_b64decode(key_str.encode())
        
        # For development, generate a key (in production this should come from secure storage)
        return Fernet.generate_key()
    
    def encrypt(self, data: str) -> Dict[str, str]:
        """Encrypt string data and return encrypted data with IV"""
        if not data:
            return {"encrypted_data": "", "iv": ""}
        
        # Generate random IV for each encryption
        iv = secrets.token_hex(16)
        
        # Encrypt the data
        encrypted_bytes = self._fernet.encrypt(data.encode('utf-8'))
        encrypted_data = base64.urlsafe_b64encode(encrypted_bytes).decode('utf-8')
        
        return {
            "encrypted_data": encrypted_data,
            "iv": iv
        }
    
    def decrypt(self, encrypted_data: str, iv: str = None) -> Optional[str]:
        """Decrypt encrypted data using IV"""
        if not encrypted_data:
            return ""
        
        try:
            # Decode the encrypted data
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode('utf-8'))
            
            # Decrypt the data
            decrypted_bytes = self._fernet.decrypt(encrypted_bytes)
            return decrypted_bytes.decode('utf-8')
        
        except Exception as e:
            # Log the error in production
            print(f"Decryption error: {e}")
            return None
    
    def encrypt_dict(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Encrypt a dictionary as JSON"""
        if not data:
            return {"encrypted_data": "", "iv": ""}
        
        json_data = json.dumps(data, sort_keys=True)
        return self.encrypt(json_data)
    
    def decrypt_dict(self, encrypted_data: str, iv: str = None) -> Optional[Dict[str, Any]]:
        """Decrypt data back to dictionary"""
        if not encrypted_data:
            return {}
        
        json_str = self.decrypt(encrypted_data, iv)
        if json_str is None:
            return None
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return None
    
    def hash_password(self, password: str, salt: Optional[str] = None) -> Dict[str, str]:
        """Hash password with salt"""
        if not salt:
            salt = secrets.token_hex(32)
        
        # Use PBKDF2 with SHA-256
        password_hash = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000  # 100k iterations
        )
        
        return {
            "password_hash": base64.urlsafe_b64encode(password_hash).decode('utf-8'),
            "salt": salt
        }
    
    def verify_password(self, password: str, password_hash: str, salt: str) -> bool:
        """Verify password against hash"""
        try:
            # Hash the provided password with the same salt
            computed_hash = hashlib.pbkdf2_hmac(
                'sha256',
                password.encode('utf-8'),
                salt.encode('utf-8'),
                100000
            )
            
            # Decode the stored hash
            stored_hash = base64.urlsafe_b64decode(password_hash.encode('utf-8'))
            
            # Use secrets.compare_digest for timing attack resistance
            return secrets.compare_digest(computed_hash, stored_hash)
        
        except Exception:
            return False
    
    def generate_token(self, length: int = 32) -> str:
        """Generate secure random token"""
        return secrets.token_urlsafe(length)
    
    def generate_api_key(self, prefix: str = "nexla") -> Dict[str, str]:
        """Generate API key with prefix"""
        key_part = secrets.token_urlsafe(32)
        full_key = f"{prefix}_{key_part}"
        
        # Create key hash for storage (don't store the full key)
        key_hash = hashlib.sha256(full_key.encode()).hexdigest()
        
        return {
            "api_key": full_key,
            "key_hash": key_hash,
            "key_prefix": f"{prefix}_{key_part[:8]}..."
        }
    
    def verify_api_key(self, provided_key: str, stored_hash: str) -> bool:
        """Verify API key against stored hash"""
        try:
            computed_hash = hashlib.sha256(provided_key.encode()).hexdigest()
            return secrets.compare_digest(computed_hash, stored_hash)
        except Exception:
            return False
    
    def encrypt_credentials(self, credentials: Dict[str, Any]) -> Dict[str, str]:
        """Encrypt credentials dictionary (Rails pattern)"""
        return self.encrypt_dict(credentials)
    
    def decrypt_credentials(self, encrypted_data: str, iv: str = None) -> Optional[Dict[str, Any]]:
        """Decrypt credentials dictionary (Rails pattern)"""
        return self.decrypt_dict(encrypted_data, iv)


# Global instance for use across the application
encryption_service = EncryptionService()