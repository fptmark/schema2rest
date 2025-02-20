# app/services/auth/cookies.py
import uuid
import json
import time
import redis
from fastapi import Request
from .base import BaseAuth, BaseCookieStore

# Configuration constants – these could be loaded from config.json later.
SESSION_TTL = 3600              # 1 hour in seconds
NEAR_EXPIRY_THRESHOLD = 300     # 5 minutes threshold

# --- Concrete Cookie Store Implementation Using Redis ---
class RedisCookieStore(BaseCookieStore):
    def __init__(self, host: str = '127.0.0.1', port: int = 6379, db: int = 0):
        self.redis_client = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    def set_session(self, session_id: str, session_data: dict, ttl: int) -> None:
        self.redis_client.setex(session_id, ttl, json.dumps(session_data))

    def get_session(self, session_id: str) -> dict:
        session_json = self.redis_client.get(session_id)
        if session_json:
            return json.loads(session_json)
        return None

    def delete_session(self, session_id: str) -> None:
        self.redis_client.delete(session_id)

    def renew_session(self, session_id: str, session_data: dict, ttl: int) -> dict:
        self.redis_client.setex(session_id, ttl, json.dumps(session_data))
        return session_data

# --- Concrete CookieAuth Implementation ---
class CookiesAuth(BaseAuth):
    # Default cookie configuration; these could be overridden by your load_config
    cookie_name = "sessionId"
    cookie_options = {
        "httponly": True,
        "secure": True,    # For local development, you might set this to False if not using HTTPS
        "samesite": "lax"
    }

    def __init__(self, cookie_store: BaseCookieStore):
        self.cookie_store = cookie_store

    def authenticate(self, request: Request) -> bool:
        """
        Returns True if a valid session exists for the token found in the cookie.
        """
        token = request.cookies.get(self.cookie_name)
        if not token:
            return False
        session = self.cookie_store.get_session(token)
        return bool(session)

    def login(self, credentials: dict) -> bool:
        """
        For now, simply validates the credentials and, if correct,
        creates a new session in the backing store.
        """
        # Placeholder: In production, replace with proper credential validation.
        if credentials.get("username") == "user" and credentials.get("password") == "pass":
            session_id = str(uuid.uuid4())
            session_data = {
                "user_id": credentials.get("username"),
                "created": time.time()
            }
            self.cookie_store.set_session(session_id, session_data, SESSION_TTL)
            # In a full implementation, you'd return the session details (e.g., the session_id)
            return True
        return False

    def logout(self, request: Request) -> None:
        token = request.cookies.get(self.cookie_name)
        if token:
            self.cookie_store.delete_session(token)

    def refresh(self, request: Request) -> bool:
        token = request.cookies.get(self.cookie_name)
        if token:
            session = self.cookie_store.get_session(token)
            if session:
                self.cookie_store.renew_session(token, session, SESSION_TTL)
                return True
        return False
