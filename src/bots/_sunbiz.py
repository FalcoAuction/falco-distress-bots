"""
Florida Division of Corporations (Sunbiz) bulk data: free daily files on
the state's public SFTP server. The state posts a shared public login on
its data-downloads page; we read it from that page at run time (or from
SUNBIZ_SFTP_USER / SUNBIZ_SFTP_PASS) rather than hard-coding it.

  https://dos.fl.gov/sunbiz/other-services/data-downloads/
  Layouts: https://dos.sunbiz.org/data-definitions/cor.html, lien.html

Fixed-width parsers below are pure (tests need no network). Transfers use
the curl binary's sftp support, which ships with Windows 10+ and Ubuntu
runners, so no new Python dependency.
"""

from __future__ import annotations

import os
import zipfile
import re
import subprocess
from typing import Any, Dict, Iterator, List, Optional, Tuple

HOST = "sftp://sftp.floridados.gov/Public/doc"
INFO_PAGE = "https://dos.fl.gov/sunbiz/other-services/data-downloads/"

# Corporate event file (COREVENTREADME): field -> (start, length), 0-based.
EVENT_FIELDS = {
    "doc_number": (0, 12), "code": (17, 20), "description": (37, 40), "effective": (77, 8), "filed": (85, 8),
    "name": (210, 192), "address1": (414, 42), "address2": (456, 42), "city": (498, 28), "state": (526, 2),
    "zip": (528, 10),
}
# Federal tax lien debtor file (lien.html, 1-based in the spec).
DEBTOR_FIELDS = {
    "filing_type": (0, 1), "doc_number": (1, 12), "name": (13, 55), "name_format": (68, 1), "address1": (69, 44),
    "address2": (113, 44), "city": (157, 28), "state": (185, 2), "zip": (187, 9), "status": (205, 1),
}
LIEN_FIELDS = {"doc_number": (0, 12), "filed": (12, 8), "status": (30, 1), "filing_type": (31, 1)}

CLOSURE = re.compile(r"DISSOLUTION|WITHDRAWAL", re.I)
NOT_CLOSURE = re.compile(r"REVOCATION|REINSTATE", re.I)


def _cut(line: str, fields: Dict[str, Tuple[int, int]]) -> Dict[str, str]:
    return {k: re.sub(r"\s+", " ", line[s:s + n]).strip() for k, (s, n) in fields.items()}


def _date(mmddyyyy: str) -> Optional[str]:
    return f"{mmddyyyy[4:8]}-{mmddyyyy[0:2]}-{mmddyyyy[2:4]}" if re.fullmatch(r"\d{8}", mmddyyyy or "") else None


def parse_events(text: str) -> List[Dict[str, Any]]:
    """Closure events only: voluntary / administrative dissolutions and
    withdrawals of foreign entities (not revocations of dissolution)."""
    out = []
    for line in text.splitlines():
        if len(line) < 420:
            continue
        r = _cut(line, EVENT_FIELDS)
        if not CLOSURE.search(r["description"]) or NOT_CLOSURE.search(r["description"]):
            continue
        r["filed"] = _date(r["filed"])
        r["effective"] = _date(r["effective"])
        r["zip"] = r["zip"][:5]
        out.append(r)
    return out


def parse_debtors(text: str) -> List[Dict[str, Any]]:
    out = []
    for line in text.splitlines():
        if len(line) < 190:
            continue
        r = _cut(line.ljust(206), DEBTOR_FIELDS)
        if r["status"] not in ("", "A"):
            continue   # lapsed or terminated
        r["zip"] = r["zip"][:5]
        r["entity"] = r["name_format"] == "C"
        out.append(r)
    return out


def parse_liens(text: str) -> Dict[str, Dict[str, Any]]:
    out = {}
    for line in text.splitlines():
        if len(line) < 32:
            continue
        r = _cut(line, LIEN_FIELDS)
        filed = r["filed"]
        r["filed"] = f"{filed[4:8]}-{filed[0:2]}-{filed[2:4]}" if re.fullmatch(r"\d{8}", filed) else None
        out[r["doc_number"]] = r
    return out


def debtor_person_name(name: str) -> str:
    """Personal debtor names come as 'LAST FIRST M'; turn them into
    'LAST, FIRST M' so the roll matcher reads them the right way round."""
    toks = name.split()
    return f"{toks[0]}, {' '.join(toks[1:])}" if len(toks) >= 2 else name


# Quarterly corporate master (cor.html), 0-based. Daily event lines carry
# only the document number for dissolutions, so names and officers come
# from here.
COR_FIELDS = {
    "doc_number": (0, 12), "name": (12, 192), "status": (204, 1), "filing_type": (205, 15),
    "address1": (220, 42), "city": (304, 28), "state": (332, 2), "zip": (334, 10),
    "officer_title": (668, 4), "officer_type": (672, 1), "officer_name": (673, 42), "officer_address": (715, 42),
    "officer_city": (757, 28), "officer_state": (785, 2), "officer_zip": (787, 9),
}
CORP_COLS = ("doc_number", "name", "status", "address", "officer", "officer_address")
QUARTERLY_COR = "Quarterly/Cor/cordata.zip"


def parse_cor_line(line: str) -> Dict[str, str]:
    r = _cut(line.ljust(800), COR_FIELDS)
    r["zip"] = r["zip"][:5]
    return r


def corp_row(r: Dict[str, str]) -> Tuple:
    addr = ", ".join(x for x in (r["address1"], r["city"], f"{r['state']} {r['zip']}".strip()) if x)
    off_addr = ", ".join(x for x in (r["officer_address"], r["officer_city"],
                                     f"{r['officer_state']} {r['officer_zip'][:5]}".strip()) if x)
    return (r["doc_number"], r["name"], r["status"], addr, r["officer_name"] or None, off_addr or None)


def remote_mtime(login: str, path: str) -> Optional[str]:
    """'Oct 07 11:45' from the long listing; changes when the state posts a new quarter."""
    folder, _, fname = path.rpartition("/")
    raw = sftp(login, folder + "/", timeout=120)
    if raw is None:
        return None
    for line in raw.decode("latin-1").splitlines():
        if line.rstrip().endswith(" " + fname):
            parts = line.split()
            return " ".join(parts[5:8]) + " " + parts[4]
    return None


# ── transport ──────────────────────────────────────────────────────────

def public_login(session) -> Optional[str]:
    user, pw = os.environ.get("SUNBIZ_SFTP_USER"), os.environ.get("SUNBIZ_SFTP_PASS")
    if user and pw:
        return f"{user}:{pw}"
    try:
        text = re.sub(r"<[^>]+>", " ", session.get(INFO_PAGE, timeout=30).text)
    except Exception:
        return None
    u = re.search(r"Username:\s*(\S+)", text)
    p = re.search(r"Password:\s*(\S+)", text)
    return f"{u.group(1)}:{p.group(1)}" if u and p else None


_CURL: List[Optional[str]] = []


def curl_bin() -> Optional[str]:
    """A curl built with sftp. Ubuntu runners' curl has it; Windows'
    System32 curl does not, Git for Windows' does."""
    if _CURL:
        return _CURL[0]
    cands = [os.environ.get("SUNBIZ_CURL"), "curl", r"C:\Program Files\Git\mingw64\bin\curl.exe"]
    found = None
    for c in cands:
        if not c:
            continue
        try:
            out = subprocess.run([c, "-V"], capture_output=True, timeout=20).stdout.decode("latin-1")
        except (OSError, subprocess.TimeoutExpired):
            continue
        if re.search(r"^Protocols:.*\bsftp\b", out, re.M):
            found = c
            break
    _CURL.append(found)
    return found


def zip_lines(path: str) -> Iterator[bytes]:
    """Lines of every member of a zip. The quarterly corporate file is
    Deflate64, which Python's zipfile cannot read; Info-ZIP unzip (Ubuntu
    runners, Git for Windows) can, so stream through it in that case."""
    try:
        with zipfile.ZipFile(path) as zf:
            for member in zf.namelist():
                with zf.open(member) as fh:
                    yield from fh
        return
    except NotImplementedError:
        pass
    for exe in ("unzip", r"C:\Program Files\Git\usr\bin\unzip.exe"):
        try:
            proc = subprocess.Popen([exe, "-p", path], stdout=subprocess.PIPE)
        except OSError:
            continue
        assert proc.stdout is not None
        yield from proc.stdout
        if proc.wait() not in (0, 1):   # 1 = warnings only
            raise RuntimeError(f"unzip exited {proc.returncode}")
        return
    raise RuntimeError("no unzip binary for a Deflate64 archive")


def sftp(login: str, path: str, listing: bool = False, timeout: int = 300,
         out_file: Optional[str] = None) -> Optional[bytes]:
    curl = curl_bin()
    if curl is None:
        return None
    args = [curl, "-s", "-k", "--max-time", str(timeout), "-u", login, f"{HOST}/{path}"]
    if listing:
        args.append("-l")
    if out_file:
        args += ["-o", out_file]
    try:
        res = subprocess.run(args, capture_output=True, timeout=timeout + 30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return res.stdout if res.returncode == 0 else None


def list_dir(login: str, path: str) -> List[str]:
    raw = sftp(login, path.rstrip("/") + "/", listing=True, timeout=120)
    if raw is None:
        return []
    return sorted(n for n in raw.decode("latin-1").split() if n not in (".", ".."))
