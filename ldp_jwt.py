# JWT
SECRET_KEY=change_me_to_a_long_random_string
JWT_ALGORITHM=HS256
TOKEN_TTL_HOURS=12
ALLOWED_DOMAIN_SUFFIX=@adroitcorp.adroit.com

# LDAP/AD
LDAP_URI=ldaps://ldap.adroitcorp.adroit.com:636
LDAP_USE_SSL=true
# If you bind directly as the service account UPN (user@domain), you don't need a bind DN.
# If your directory requires searching first, set these to a service account that can search.
LDAP_BIND_DN=  # e.g. CN=svc-ldap,OU=Service Accounts,DC=adroitcorp,DC=adroit,DC=com
LDAP_BIND_PASSWORD=
LDAP_BASE_DN=DC=adroitcorp,DC=adroit,DC=com

# Ozone S3
OZONE_S3_ENDPOINT=http://ozone-s3-gateway:9878
OZONE_S3_ACCESS_KEY=YOUR_OZONE_S3_ACCESS_KEY
OZONE_S3_SECRET_KEY=YOUR_OZONE_S3_SECRET_KEY
OZONE_BUCKET=documents


#=======
import os
import re
import jwt
import boto3
import datetime
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Header, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from ldap3 import Server, Connection, Tls, ALL, ALL_ATTRIBUTES
from dotenv import load_dotenv

load_dotenv()

# ---------------- Config ----------------
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-insecure")
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
TOKEN_TTL_HOURS = int(os.environ.get("TOKEN_TTL_HOURS", "12"))
ALLOWED_DOMAIN_SUFFIX = os.environ.get("ALLOWED_DOMAIN_SUFFIX", "@adroitcorp.adroit.com")

LDAP_URI = os.environ.get("LDAP_URI", "ldaps://ldap.example.com:636")
LDAP_USE_SSL = os.environ.get("LDAP_USE_SSL", "true").lower() == "true"
LDAP_BIND_DN = os.environ.get("LDAP_BIND_DN", "")        # optional (for search-first)
LDAP_BIND_PASSWORD = os.environ.get("LDAP_BIND_PASSWORD", "")
LDAP_BASE_DN = os.environ.get("LDAP_BASE_DN", "")

OZONE_S3_ENDPOINT = os.environ.get("OZONE_S3_ENDPOINT", "http://ozone-s3-gateway:9878")
OZONE_S3_ACCESS_KEY = os.environ.get("OZONE_S3_ACCESS_KEY", "")
OZONE_S3_SECRET_KEY = os.environ.get("OZONE_S3_SECRET_KEY", "")
OZONE_BUCKET = os.environ.get("OZONE_BUCKET", "documents")

# Initialize Ozone S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=OZONE_S3_ENDPOINT,
    aws_access_key_id=OZONE_S3_ACCESS_KEY,
    aws_secret_access_key=OZONE_S3_SECRET_KEY,
)

app = FastAPI(title="JWT + LDAP + Ozone API")

# ---------------- Models ----------------
class LoginBody(BaseModel):
    service_account: str
    password: str

# ---------------- Utilities ----------------
def _ensure_allowed_service_account(service_account: str) -> None:
    """
    Enforce your domain policy: service account must end with the allowed suffix.
    """
    if not service_account or not service_account.endswith(ALLOWED_DOMAIN_SUFFIX):
        raise HTTPException(status_code=403, detail="Service account domain not allowed")

def _sanitize_prefix_from_service_account(service_account: str) -> str:
    """
    Derive a safe S3 key prefix from the account name, e.g., 'teamA@domain' -> 'teamA'.
    Avoid strange characters for S3 keys.
    """
    name = service_account.split("@")[0]
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", name)
    return safe

def _ldap_server():
    # You can add TLS cert validation here if needed
    tls = Tls() if LDAP_USE_SSL else None
    return Server(LDAP_URI, get_info=ALL, use_ssl=LDAP_USE_SSL, tls=tls)

def ldap_authenticate(service_account: str, password: str) -> bool:
    """
    Authenticate against LDAP/AD.

    Two modes supported:
    1) Direct bind using UPN (works well with AD): user=service_account, password=password
    2) Search-first (if LDAP requires DN): bind with LDAP_BIND_DN, search for user DN, then bind as that DN
    """
    server = _ldap_server()

    # (1) Try direct bind with UPN (userPrincipalName) — typical for AD
    try:
        conn = Connection(server, user=service_account, password=password, auto_bind=True)
        conn.unbind()
        return True
    except Exception:
        # Fallback to search-first if configured
        pass

    # (2) Search-first approach
    if not LDAP_BIND_DN or not LDAP_BIND_PASSWORD or not LDAP_BASE_DN:
        # If we can't search, fail early
        return False

    try:
        # Bind with a service account to search
        admin = Connection(server, user=LDAP_BIND_DN, password=LDAP_BIND_PASSWORD, auto_bind=True)
        # Search for the user by UPN (AD) or mail/uid/etc. Adjust filter to your directory schema.
        search_filter = f"(userPrincipalName={service_account})"
        admin.search(search_base=LDAP_BASE_DN, search_filter=search_filter, attributes=ALL_ATTRIBUTES)
        if len(admin.entries) != 1:
            admin.unbind()
            return False

        user_dn = admin.entries[0].entry_dn
        admin.unbind()

        # Now try to bind as the user DN with the provided password
        user_conn = Connection(server, user=user_dn, password=password, auto_bind=True)
        user_conn.unbind()
        return True
    except Exception:
        return False

def mint_jwt(service_account: str) -> str:
    exp = datetime.datetime.utcnow() + datetime.timedelta(hours=TOKEN_TTL_HOURS)
    payload = {"sub": service_account, "exp": exp}
    return jwt.encode(payload, SECRET_KEY, algorithm=JWT_ALGORITHM)

def verify_jwt_and_get_subject(authorization: str = Header(...)) -> str:
    """
    Validates:
      - Bearer header format
      - Signature with SECRET_KEY
      - Exp within time
      - Service account suffix policy
    Returns the 'sub' (service_account) if valid.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = authorization[len("Bearer "):].strip()

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidSignatureError:
        raise HTTPException(status_code=401, detail="Invalid token signature")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    service_account = payload.get("sub")
    _ensure_allowed_service_account(service_account)
    return service_account

# ---------------- Endpoints ----------------
@app.post("/auth/login")
def login(body: LoginBody):
    """
    Step 1: Validate service account format.
    Step 2: LDAP authenticate (username/password).
    Step 3: Mint a signed JWT on success.
    """
    _ensure_allowed_service_account(body.service_account)

    if not ldap_authenticate(body.service_account, body.password):
        raise HTTPException(status_code=401, detail="LDAP authentication failed")

    token = mint_jwt(body.service_account)
    return {"access_token": token, "token_type": "Bearer", "expires_in_hours": TOKEN_TTL_HOURS}

@app.get("/me")
def me(service_account: str = Depends(verify_jwt_and_get_subject)):
    """
    Verify JWT and return caller identity (service account).
    """
    return {"service_account": service_account}

@app.post("/upload")
def upload(file: UploadFile = File(...), service_account: str = Depends(verify_jwt_and_get_subject)):
    """
    Upload a file to Ozone under a folder/prefix derived from the service account.
    Ranger can enforce bucket-level policies; this API also isolates by prefix.
    """
    prefix = _sanitize_prefix_from_service_account(service_account)
    key = f"{prefix}/{file.filename}"
    try:
        s3.upload_fileobj(file.file, OZONE_BUCKET, key)
        return {"message": f"Uploaded: s3://{OZONE_BUCKET}/{key}", "by": service_account}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

@app.get("/download/{filename}")
def download(filename: str, service_account: str = Depends(verify_jwt_and_get_subject)):
    """
    Download a file only from the caller's own prefix.
    """
    prefix = _sanitize_prefix_from_service_account(service_account)
    key = f"{prefix}/{filename}"
    try:
        obj = s3.get_object(Bucket=OZONE_BUCKET, Key=key)
        return StreamingResponse(
            obj["Body"],
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Download failed: {e}")