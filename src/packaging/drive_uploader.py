import os
from typing import Optional


def have_drive_creds() -> bool:
    """
    Determines if Google Drive service account creds are present.
    We support either:
      - GOOGLE_SERVICE_ACCOUNT_JSON (full json string)
      - GOOGLE_SERVICE_ACCOUNT_JSON_B64 (base64 json string)
    and a target folder id:
      - GOOGLE_DRIVE_FOLDER_ID
    """
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    sa_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()

    if not folder_id:
        return False
    if sa_json or sa_b64:
        return True
    return False


def upload_pdf(local_pdf_path: str, filename: str) -> Optional[str]:
    """
    Uploads a local PDF to Google Drive and returns a shareable URL.

    IMPORTANT:
    - If creds are missing, we do NOT error.
    - We return None and let the caller decide how to handle it.
    """
    if not have_drive_creds():
        print("[DriveUploader] WARNING: missing Google service account creds; skipping Drive upload.")
        return None

    # Lazy import so missing deps don't crash packaging runs.
    try:
        from .google_drive import upload_file_to_drive  # type: ignore
    except Exception as e:
        print(f"[DriveUploader] WARNING: Drive upload module unavailable; skipping upload. err={type(e).__name__}: {e}")
        return None

    try:
        url = upload_file_to_drive(local_pdf_path=local_pdf_path, filename=filename)
        return url
    except Exception as e:
        print(f"[DriveUploader] ERROR: upload failed (non-fatal). err={type(e).__name__}: {e}")
        return None
