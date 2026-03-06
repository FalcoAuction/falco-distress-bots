# -*- coding: utf-8 -*-
# src/enrichment/notice_pdf.py
import base64
import re
from typing import Optional, Tuple

import requests


def extract_embedded_pdf_bytes(html: str) -> Optional[bytes]:
    """
    ForeclosureTennessee embeds the notice PDF as:
      href="data:application/pdf;base64,...."
    Returns PDF bytes or None if not found/invalid.
    """
    if not html:
        return None
    m = re.search(r'href="data:application/pdf;base64,([^"]+)"', html, flags=re.I)
    if not m:
        return None
    b64 = re.sub(r"\s+", "", m.group(1))
    try:
        pdf = base64.b64decode(b64)
    except Exception:
        return None
    # sanity check
    if not pdf.startswith(b"%PDF-"):
        return None
    return pdf


def fetch_notice_pdf(url: str, timeout: int = 30) -> Tuple[Optional[bytes], str]:
    """
    Fetch listing HTML and extract embedded PDF bytes.
    Returns (pdf_bytes_or_none, status_string)
    status_string: ok | no_pdf | http_error | invalid_pdf
    """
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        pdf = extract_embedded_pdf_bytes(r.text or "")
        if pdf is None:
            return None, "no_pdf"
        return pdf, "ok"
    except requests.RequestException:
        return None, "http_error"
    except Exception:
        return None, "invalid_pdf"
