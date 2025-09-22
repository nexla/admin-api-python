# Authentication module
from .jwt_auth import JWTAuth, PasswordUtils, get_current_user, AuthService

# Expose commonly used functions
create_access_token = JWTAuth.create_access_token
verify_password = PasswordUtils.verify_password  
get_password_hash = PasswordUtils.hash_password
verify_token = JWTAuth.verify_token
authenticate_user = AuthService.authenticate_user