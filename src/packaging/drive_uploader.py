# src/packaging/drive_uploader.py
import os
import json
import base64
import tempfile
import subprocess
import time
from typing import Any, Dict, Optional, Tuple

import requests

# ============================================================
# Service Account Auth WITHOUT extra deps
# - Uses openssl for RS256 signing (available on GitHub Actions runners)
# - Exchanges JWT for OAuth access token
# ============================================================

DRIVE_SCOPES = "https://www.googleapis.com/auth/drive.file"
TOKEN_URL = "https://oauth2.googleapis.com/token"
UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"
FILES_URL = "https://www.googleapis.com/drive/v3/files"
PERMS_URL = "https://www.googleapis.com/drive/v3/files/{file_id}/permissions"

DEBUG = os.getenv("FALCO_PDF_DEBUG", "").strip() not in ("", "0", "false", "False")


def _load_service_account() -> Optional[dict]:
    raw = os.getenv("FALCO_GOOGLE_SA_JSON", "").strip()
    path = os.getenv("FALCO_GOOGLE_SA_JSON_PATH", "").strip()
    if raw:
        try:
            return json.loads(raw)
        except Exception as e:
            print(f"[DriveUploader] WARNING: invalid FALCO_GOOGLE_SA_JSON ({type(e).__name__}: {e})")
            return None
    if path:
        if not os.path.exists(path):
            print(f"[DriveUploader] WARNING: FALCO_GOOGLE_SA_JSON_PATH not found: {path}")
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.loads(f.read())
        except Exception as e:
            print(f"[DriveUploader] WARNING: failed reading service account JSON ({type(e).__name__}: {e})")
            return None
    return None


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _sign_rs256(message: bytes, private_key_pem: str) -> bytes:
    # Use openssl to avoid adding cryptography deps
    with tempfile.TemporaryDirectory() as td:
        key_path = os.path.join(td, "sa.key")
        msg_path = os.path.join(td, "msg.bin")
        sig_path = os.path.join(td, "sig.bin")

        with open(key_path, "w", encoding="utf-8") as f:
            f.write(private_key_pem)
        with open(msg_path, "wb") as f:
            f.write(message)

        cmd = ["openssl", "dgst", "-sha256", "-sign", key_path, "-out", sig_path, msg_path]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            raise RuntimeError(f"openssl sign failed: {proc.stderr.decode('utf-8', errors='ignore')[:300]}")
        with open(sig_path, "rb") as f:
            return f.read()


def _make_jwt(sa: dict) -> str:
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    payload = {
        "iss": sa.get("client_email"),
        "scope": DRIVE_SCOPES,
        "aud": TOKEN_URL,
        "iat": now,
        "exp": now + 3600,
    }
    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")

    sig = _sign_rs256(signing_input, sa.get("private_key", ""))
    sig_b64 = _b64url(sig)
    return f"{header_b64}.{payload_b64}.{sig_b64}"


def get_access_token() -> Optional[str]:
    sa = _load_service_account()
    if not sa:
        return None

    try:
        jwt = _make_jwt(sa)
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt,
        }
        r = requests.post(TOKEN_URL, data=data, timeout=30)
        if r.status_code >= 300:
            print(f"[DriveUploader] WARNING: token exchange failed ({r.status_code}).")
            return None
        return (r.json() or {}).get("access_token")
    except Exception as e:
        print(f"[DriveUploader] WARNING: failed to get access token ({type(e).__name__}: {e})")
        return None


def _multipart_body(metadata: dict, file_bytes: bytes, filename: str, mime: str = "application/pdf") -> Tuple[bytes, str]:
    boundary = "-------falcoBoundary7MA4YWxkTrZu0gW"
    parts = []
    parts.append(f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{json.dumps(metadata)}\r\n")
    parts.append(f"--{boundary}\r\nContent-Type: {mime}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\r\n")
    body = "".join(parts).encode("utf-8") + file_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")
    return body, boundary


def upload_pdf(pdf_path: str, *, folder_id: Optional[str] = None, make_public: bool = True) -> Optional[str]:
    token = get_access_token()
    if not token:
        print("[DriveUploader] WARNING: missing Google service account creds; skipping Drive upload.")
        return None

    if not os.path.exists(pdf_path):
        print(f"[DriveUploader] WARNING: PDF not found: {pdf_path}")
        return None

    filename = os.path.basename(pdf_path)
    with open(pdf_path, "rb") as f:
        file_bytes = f.read()

    metadata: Dict[str, Any] = {"name": filename}
    if folder_id:
        metadata["parents"] = [folder_id]

    body, boundary = _multipart_body(metadata, file_bytes, filename)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": f"multipart/related; boundary={boundary}",
    }

    try:
        r = requests.post(UPLOAD_URL, headers=headers, data=body, timeout=60)
        if r.status_code >= 300:
            print(f"[DriveUploader] WARNING: upload failed ({r.status_code}).")
            if DEBUG:
                print(r.text[:500])
            return None
        file_id = (r.json() or {}).get("id")
        if not file_id:
            return None

        if make_public:
            pr = requests.post(
                PERMS_URL.format(file_id=file_id),
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"role": "reader", "type": "anyone"},
                timeout=30,
            )
            if pr.status_code >= 300 and DEBUG:
                print(f"[DriveUploader] permission warning ({pr.status_code}): {pr.text[:300]}")

        # fetch webViewLink
        gr = requests.get(
            f"{FILES_URL}/{file_id}?fields=webViewLink",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if gr.status_code >= 300:
            return None
        return (gr.json() or {}).get("webViewLink")

    except Exception as e:
        print(f"[DriveUploader] WARNING: upload exception ({type(e).__name__}: {e})")
        return None
