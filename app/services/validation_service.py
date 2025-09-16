from typing import Dict, Any, List, Optional
from datetime import datetime
import re
import json


class ValidationService:
    """Service for validating various data types and configurations"""
    
    @staticmethod
    def validate_email(email: str) -> Dict[str, Any]:
        """Validate email address format"""
        if not email or not email.strip():
            return {"valid": False, "errors": ["Email is required"]}
        
        email = email.strip().lower()
        
        if len(email) > 254:
            return {"valid": False, "errors": ["Email is too long (max 254 characters)"]}
        
        # Basic email regex pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            return {"valid": False, "errors": ["Invalid email format"]}
        
        return {"valid": True, "email": email, "errors": []}
    
    @staticmethod
    def validate_json_config(config: Any) -> Dict[str, Any]:
        """Validate JSON configuration"""
        if config is None:
            return {"valid": True, "config": None, "errors": []}
        
        if isinstance(config, dict):
            return {"valid": True, "config": config, "errors": []}
        
        if isinstance(config, str):
            try:
                parsed = json.loads(config)
                return {"valid": True, "config": parsed, "errors": []}
            except json.JSONDecodeError as e:
                return {"valid": False, "errors": [f"Invalid JSON: {str(e)}"]}
        
        return {"valid": False, "errors": ["Configuration must be a JSON object or string"]}
    
    @staticmethod
    def validate_url(url: str, schemes: Optional[List[str]] = None) -> Dict[str, Any]:
        """Validate URL format"""
        if not url or not url.strip():
            return {"valid": False, "errors": ["URL is required"]}
        
        url = url.strip()
        schemes = schemes or ['http', 'https']
        
        # Basic URL pattern
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if not re.match(url_pattern, url):
            return {"valid": False, "errors": ["Invalid URL format"]}
        
        # Check scheme
        scheme = url.split('://')[0].lower()
        if scheme not in schemes:
            return {"valid": False, "errors": [f"URL scheme must be one of: {', '.join(schemes)}"]}
        
        return {"valid": True, "url": url, "errors": []}
    
    @staticmethod
    def validate_name(name: str, min_length: int = 1, max_length: int = 255) -> Dict[str, Any]:
        """Validate name field"""
        if not name or not name.strip():
            return {"valid": False, "errors": ["Name is required"]}
        
        name = name.strip()
        
        if len(name) < min_length:
            return {"valid": False, "errors": [f"Name must be at least {min_length} characters"]}
        
        if len(name) > max_length:
            return {"valid": False, "errors": [f"Name must be no more than {max_length} characters"]}
        
        # Allow letters, numbers, spaces, hyphens, underscores, dots, parentheses
        if not re.match(r'^[\w\s\-\.\(\)]+$', name):
            return {"valid": False, "errors": ["Name contains invalid characters"]}
        
        return {"valid": True, "name": name, "errors": []}
    
    @staticmethod
    def validate_password(email: str, full_name: str, password: str) -> Dict[str, Any]:
        """Validate password strength with context"""
        if not password:
            return {"valid": False, "message": "Password is required", "errors": ["Password is required"]}
        
        errors = []
        
        # Basic length requirement
        if len(password) < 8:
            errors.append("Password must be at least 8 characters long")
        
        # Character requirements
        if not re.search(r'[a-z]', password):
            errors.append("Password must contain at least one lowercase letter")
        
        if not re.search(r'[A-Z]', password):
            errors.append("Password must contain at least one uppercase letter")
        
        if not re.search(r'\d', password):
            errors.append("Password must contain at least one digit")
        
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            errors.append("Password must contain at least one special character")
        
        # Context-based validation
        if email and email.lower() in password.lower():
            errors.append("Password should not contain your email address")
        
        if full_name and len(full_name) > 2:
            name_parts = full_name.lower().split()
            for part in name_parts:
                if len(part) > 2 and part in password.lower():
                    errors.append("Password should not contain parts of your name")
                    break
        
        # Common weak passwords
        weak_passwords = ['password', '12345678', 'qwerty123', 'admin123', 'letmein']
        if password.lower() in weak_passwords:
            errors.append("Password is too common, please choose a stronger password")
        
        # Calculate entropy score
        entropy_score = ValidationService._calculate_password_entropy(password)
        
        is_valid = len(errors) == 0
        message = "Password is strong" if is_valid else "; ".join(errors)
        
        return {
            "valid": is_valid, 
            "message": message,
            "errors": errors,
            "entropy_score": entropy_score,
            "strength": ValidationService._get_password_strength(entropy_score)
        }
    
    @staticmethod
    def _calculate_password_entropy(password: str) -> float:
        """Calculate password entropy score"""
        import math
        
        # Character set size based on what's actually in the password
        charset_size = 0
        if re.search(r'[a-z]', password):
            charset_size += 26
        if re.search(r'[A-Z]', password):
            charset_size += 26
        if re.search(r'\d', password):
            charset_size += 10
        if re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            charset_size += 32  # approximate special chars
        
        if charset_size == 0:
            return 0
        
        # Entropy = log2(charset_size^length)
        entropy = len(password) * math.log2(charset_size)
        return round(entropy, 2)
    
    @staticmethod
    def _get_password_strength(entropy_score: float) -> str:
        """Get password strength label based on entropy"""
        if entropy_score < 25:
            return "Very Weak"
        elif entropy_score < 35:
            return "Weak"
        elif entropy_score < 50:
            return "Fair"
        elif entropy_score < 75:
            return "Good"
        else:
            return "Strong"
    
    @staticmethod
    def validate_tags(tags: Any) -> Dict[str, Any]:
        """Validate tags list"""
        if tags is None:
            return {"valid": True, "tags": [], "errors": []}
        
        if isinstance(tags, str):
            # Handle comma-separated string
            tag_list = [tag.strip() for tag in tags.split(',') if tag.strip()]
        elif isinstance(tags, (list, tuple)):
            tag_list = [str(tag).strip() for tag in tags if str(tag).strip()]
        else:
            return {"valid": False, "errors": ["Tags must be a list or comma-separated string"]}
        
        # Validate individual tags
        errors = []
        for tag in tag_list:
            if len(tag) > 50:
                errors.append(f"Tag '{tag}' is too long (max 50 characters)")
            if not re.match(r'^[\w\-\.]+$', tag):
                errors.append(f"Tag '{tag}' contains invalid characters")
        
        return {"valid": len(errors) == 0, "tags": tag_list, "errors": errors}
    
    @staticmethod
    def validate_cron_expression(cron: str) -> Dict[str, Any]:
        """Validate cron expression format"""
        if not cron or not cron.strip():
            return {"valid": False, "errors": ["Cron expression is required"]}
        
        cron = cron.strip()
        parts = cron.split()
        
        if len(parts) != 5:
            return {"valid": False, "errors": ["Cron expression must have 5 parts (minute hour day month weekday)"]}
        
        # Basic validation for each part
        minute, hour, day, month, weekday = parts
        
        errors = []
        
        # Validate minute (0-59)
        if not ValidationService._validate_cron_part(minute, 0, 59):
            errors.append("Invalid minute field (0-59)")
        
        # Validate hour (0-23)
        if not ValidationService._validate_cron_part(hour, 0, 23):
            errors.append("Invalid hour field (0-23)")
        
        # Validate day (1-31)
        if not ValidationService._validate_cron_part(day, 1, 31):
            errors.append("Invalid day field (1-31)")
        
        # Validate month (1-12)
        if not ValidationService._validate_cron_part(month, 1, 12):
            errors.append("Invalid month field (1-12)")
        
        # Validate weekday (0-7, where both 0 and 7 represent Sunday)
        if not ValidationService._validate_cron_part(weekday, 0, 7):
            errors.append("Invalid weekday field (0-7)")
        
        return {"valid": len(errors) == 0, "cron": cron, "errors": errors}
    
    @staticmethod
    def _validate_cron_part(part: str, min_val: int, max_val: int) -> bool:
        """Validate individual cron expression part"""
        if part == '*':
            return True
        
        # Handle ranges (e.g., "1-5")
        if '-' in part:
            try:
                start, end = part.split('-', 1)
                start_val, end_val = int(start), int(end)
                return min_val <= start_val <= max_val and min_val <= end_val <= max_val and start_val <= end_val
            except ValueError:
                return False
        
        # Handle step values (e.g., "*/5" or "0-23/2")
        if '/' in part:
            try:
                range_part, step = part.split('/', 1)
                step_val = int(step)
                if step_val <= 0:
                    return False
                
                if range_part == '*':
                    return True
                elif '-' in range_part:
                    start, end = range_part.split('-', 1)
                    start_val, end_val = int(start), int(end)
                    return min_val <= start_val <= max_val and min_val <= end_val <= max_val and start_val <= end_val
                else:
                    val = int(range_part)
                    return min_val <= val <= max_val
            except ValueError:
                return False
        
        # Handle comma-separated values (e.g., "1,3,5")
        if ',' in part:
            try:
                values = [int(v.strip()) for v in part.split(',')]
                return all(min_val <= v <= max_val for v in values)
            except ValueError:
                return False
        
        # Handle single value
        try:
            val = int(part)
            return min_val <= val <= max_val
        except ValueError:
            return False