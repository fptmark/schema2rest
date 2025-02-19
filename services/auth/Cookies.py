import uuid
import json
import time
import redis
from fastapi import APIRouter, Request, Response, HTTPException
from pydantic import BaseModel

# ---------------------------
# Redis Session Store Setup
# ---------------------------
redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)

# Session configuration constants
SESSION_TTL = 3600              # Session time-to-live in seconds (1 hour)
NEAR_EXPIRY_THRESHOLD = 300     # Threshold (in seconds) to consider a session near expiry

# ---------------------------
# Helper Functions for Sessions
# ---------------------------
def validate_credentials(username: str, password: str) -> bool:
    """
    Validate user credentials.
    In production, replace this with secure password verification.
    """
    return username == "user" and password == "pass"

def create_new_session(user_id: str) -> dict:
    """
    Creates a new session in Redis with a unique session ID.
    """
    session_id = str(uuid.uuid4())
    session_data = {
        "user_id": user_id,
        "created": time.time()
    }
    redis_client.setex(session_id, SESSION_TTL, json.dumps(session_data))
    session_data["id"] = session_id
    return session_data

def renew_session(session_id: str, session_data: dict) -> dict:
    """
    Renews a session by resetting its TTL.
    """
    redis_client.setex(session_id, SESSION_TTL, json.dumps(session_data))
    return session_data

def get_session(session_id: str) -> dict:
    """
    Retrieves the session data from Redis.
    """
    session_json = redis_client.get(session_id)
    if session_json:
        return json.loads(session_json)
    return None

def delete_session(session_id: str) -> None:
    """
    Deletes the session from Redis.
    """
    redis_client.delete(session_id)

def is_near_expiry(session_id: str) -> bool:
    """
    Checks if a session is near its expiration threshold.
    """
    ttl = redis_client.ttl(session_id)
    return ttl is not None and ttl < NEAR_EXPIRY_THRESHOLD

# ---------------------------
# Pydantic Model for Login Request
# ---------------------------
class LoginRequest(BaseModel):
    username: str
    password: str

# ---------------------------
# CookieAuth Model Definition
# ---------------------------
class CookieAuth:
    cookie_name = "sessionId"
    cookie_options = {
        "httponly": True,
        "secure": True,   # For local development, set to False if not using HTTPS
        "samesite": "lax"
    }

    @classmethod
    def authenticate(cls, request: Request) -> dict:
        """
        Validates the incoming request by checking the session cookie.
        """
        token = request.cookies.get(cls.cookie_name)
        if not token:
            return None
        session = get_session(token)
        return session

    @classmethod
    def login(cls, credentials: LoginRequest) -> dict:
        """
        Validates credentials and creates a new session if valid.
        """
        if validate_credentials(credentials.username, credentials.password):
            session = create_new_session(user_id=credentials.username)
            return session
        return None

    @classmethod
    def logout(cls, request: Request) -> None:
        """
        Logs out the user by deleting the session.
        """
        token = request.cookies.get(cls.cookie_name)
        if token:
            delete_session(token)

    @classmethod
    def refresh(cls, request: Request) -> dict:
        """
        Renews the session if it's near expiry.
        """
        token = request.cookies.get(cls.cookie_name)
        if token and is_near_expiry(token):
            session = get_session(token)
            if session:
                renewed_session = renew_session(token, session)
                return renewed_session
        return None

# ---------------------------
# FastAPI Router Definition
# ---------------------------
router = APIRouter(prefix="/auth", tags=["Authentication"])

@router.post("/login")
async def login_endpoint(credentials: LoginRequest, response: Response):
    session = CookieAuth.login(credentials)
    if session:
        response.set_cookie(
            key=CookieAuth.cookie_name,
            value=session["id"],
            max_age=SESSION_TTL,
            httponly=CookieAuth.cookie_options["httponly"],
            secure=CookieAuth.cookie_options["secure"],
            samesite=CookieAuth.cookie_options["samesite"]
        )
        return {"message": "Login successful", "user": session["user_id"]}
    raise HTTPException(status_code=401, detail="Invalid credentials")

@router.post("/logout")
async def logout_endpoint(request: Request, response: Response):
    CookieAuth.logout(request)
    response.delete_cookie(CookieAuth.cookie_name)
    return {"message": "Logged out successfully"}

@router.post("/refresh")
async def refresh_endpoint(request: Request, response: Response):
    renewed_session = CookieAuth.refresh(request)
    if renewed_session:
        response.set_cookie(
            key=CookieAuth.cookie_name,
            value=request.cookies.get(CookieAuth.cookie_name),
            max_age=SESSION_TTL,
            httponly=CookieAuth.cookie_options["httponly"],
            secure=CookieAuth.cookie_options["secure"],
            samesite=CookieAuth.cookie_options["samesite"]
        )
        return {"message": "Session refreshed"}
    raise HTTPException(status_code=400, detail="Session not found or not near expiry")

@router.get("/protected")
async def protected_endpoint(request: Request):
    session = CookieAuth.authenticate(request)
    if session:
        return {"message": "Access granted", "user": session["user_id"]}
    raise HTTPException(status_code=401, detail="Unauthorized")
