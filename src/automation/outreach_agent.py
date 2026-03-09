from __future__ import annotations

import argparse
import csv
import json
import os
import re
import smtplib
import ssl
from dataclasses import asdict, dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup


SEARCH_URL = "https://html.duckduckgo.com/html/"
USER_AGENT = "Mozilla/5.0 (Falco Outreach Agent)"
TIMEOUT = 20
MAX_SITE_CHARS = 20000


@dataclass
class Candidate:
    track: str
    rank: int
    score: int
    organization: str
    contact_name: str
    email: str
    website: str
    domain: str
    city: str
    state: str
    reason: str
    snippet: str
    personalized_line: str
    subject: str
    body: str
    query: str


TRACK_QUERIES = {
    "auction_partner": [
        "Tennessee real estate auction company Nashville foreclosure auction",
        "Middle Tennessee auction company real estate foreclosure sales",
        "Tennessee auctioneer real estate distressed property investors",
        "Nashville foreclosure auction investor auction company",
        "Tennessee trustee sale auction company real estate",
    ],
    "principal_broker": [
        "Tennessee principal broker Nashville real estate brokerage",
        "Middle Tennessee managing broker real estate firm Nashville",
        "Tennessee broker owner real estate company Nashville Franklin",
        "Tennessee principal broker Clarksville Murfreesboro realty",
        "Tennessee real estate brokerage executive principal broker",
    ],
}

TRACK_SEED_FILES = {
    "auction_partner": Path("data") / "seeds" / "auction_partner_targets.csv",
    "principal_broker": Path("data") / "seeds" / "principal_broker_targets.csv",
}


TRACK_KEYWORDS = {
    "auction_partner": [
        "auction",
        "foreclosure",
        "real estate",
        "distressed",
        "investor",
        "trustee",
        "tax sale",
        "court sale",
    ],
    "principal_broker": [
        "principal broker",
        "managing broker",
        "broker",
        "real estate",
        "realty",
        "brokerage",
        "office",
        "tennessee",
    ],
}

BLOCKED_DOMAINS = {
    "foreclosurelistings.com",
    "distressedpropertiessale.com",
    "keycrew.co",
}

BLOCKED_EMAIL_PATTERNS = (
    "example.com",
    "test.com",
    "wixpress.com",
    "sentry.io",
)

TRACK_BLOCKLIST_TERMS = {
    "auction_partner": [
        "directory",
        "listing service",
        "foreclosure listings",
        "handyman special",
        "providers/",
    ],
    "principal_broker": [
        "directory",
        "providers/",
        "find vetted",
        "agent marketplace",
        "referral network",
        "course",
        "education",
        "institute",
        "association",
        "school",
        "career",
    ],
}


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    return host.removeprefix("www.")


def _real_result_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("//"):
        return f"https:{href}"
    if href.startswith("http://") or href.startswith("https://"):
        return href
    parsed = urlparse(href)
    if parsed.path == "/l/":
        target = parse_qs(parsed.query).get("uddg")
        if target:
            return target[0]
    return href


def _search(query: str, limit: int = 20) -> list[dict[str, str]]:
    resp = requests.post(
        SEARCH_URL,
        data={"q": query},
        headers={"User-Agent": USER_AGENT},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    results: list[dict[str, str]] = []
    for block in soup.select(".result"):
        link = block.select_one(".result__a")
        if not link:
            continue
        url = _real_result_url(link.get("href", ""))
        if not url.startswith("http"):
            continue
        title = _clean_text(link.get_text(" ", strip=True))
        snippet_el = block.select_one(".result__snippet")
        snippet = _clean_text(snippet_el.get_text(" ", strip=True) if snippet_el else "")
        results.append({"title": title, "url": url, "snippet": snippet, "query": query})
        if len(results) >= limit:
            break
    return results


def _load_seed_rows(track: str) -> list[dict[str, str]]:
    path = TRACK_SEED_FILES[track]
    if not path.is_file():
        return []
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("website") or "").strip()
            if not url.startswith("http"):
                continue
            rows.append(
                {
                    "title": (row.get("organization") or "").strip(),
                    "url": url,
                    "snippet": (row.get("notes") or "").strip(),
                    "query": "seed",
                }
            )
    return rows


def _extract_emails(text: str) -> list[str]:
    found = {
        email.strip(" .;,)")
        for email in re.findall(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", text or "")
    }
    filtered = [
        email
        for email in sorted(found)
        if not any(skip in email.lower() for skip in BLOCKED_EMAIL_PATTERNS)
        and not email.lower().endswith((".png", ".jpg", ".jpeg", ".svg", ".webp", ".gif"))
        and "asset-" not in email.lower()
    ]
    return filtered


def _extract_city_state(text: str) -> tuple[str, str]:
    text = text or ""
    patterns = [
        r"\b(Nashville|Brentwood|Franklin|Murfreesboro|Clarksville|Goodlettsville|Hendersonville|Gallatin|Mt\.?\s*Juliet|Mount Juliet),?\s*(TN|Tennessee)\b",
        r"\b(TN|Tennessee)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        groups = [g for g in match.groups() if g]
        if len(groups) == 2:
            return _clean_text(groups[0]), "TN"
        return "", "TN"
    return "", ""


def _find_same_domain_links(base_url: str, soup: BeautifulSoup) -> list[str]:
    base_domain = _domain(base_url)
    links: list[str] = []
    for a in soup.select("a[href]"):
        href = urljoin(base_url, a.get("href", ""))
        if _domain(href) != base_domain:
            continue
        text = _clean_text(a.get_text(" ", strip=True)).lower()
        href_lower = href.lower()
        if any(token in text or token in href_lower for token in ("contact", "about", "team", "staff", "broker", "auctioneer")):
            links.append(href)
    deduped: list[str] = []
    seen = set()
    for href in links:
        if href in seen:
            continue
        seen.add(href)
        deduped.append(href)
        if len(deduped) >= 3:
            break
    return deduped


def _fetch_page(url: str) -> tuple[str, BeautifulSoup] | tuple[str, None]:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        if resp.status_code >= 400:
            return "", None
        html = resp.text[:MAX_SITE_CHARS]
        return html, BeautifulSoup(html, "html.parser")
    except Exception:
        return "", None


def _page_context(url: str) -> dict[str, str | list[str]]:
    html, soup = _fetch_page(url)
    if not soup:
        return {
            "organization": _domain(url).split(".")[0].replace("-", " ").title(),
            "emails": [],
            "city": "",
            "state": "",
            "summary": "",
        }

    title = _clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")
    meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    description = _clean_text(meta.get("content", "") if meta else "")
    visible_text = _clean_text(soup.get_text(" ", strip=True))[:MAX_SITE_CHARS]
    emails = _extract_emails(html + "\n" + visible_text)

    for extra_url in _find_same_domain_links(url, soup):
        extra_html, extra_soup = _fetch_page(extra_url)
        if not extra_soup:
            continue
        extra_text = _clean_text(extra_soup.get_text(" ", strip=True))
        emails = sorted(set(emails + _extract_emails(extra_html + "\n" + extra_text)))
        if emails:
            break

    org = title.split("|")[0].split(" - ")[0].strip() if title else _domain(url).split(".")[0].replace("-", " ").title()
    city, state = _extract_city_state(" ".join([title, description, visible_text[:2000]]))
    summary = description or visible_text[:300]
    return {
        "organization": org or _domain(url).split(".")[0].replace("-", " ").title(),
        "emails": emails,
        "city": city,
        "state": state,
        "summary": summary,
    }


def _domain_email_match(domain: str, email: str) -> bool:
    if not email:
        return False
    email_domain = email.split("@", 1)[-1].lower()
    return email_domain == domain or email_domain.endswith(f".{domain}")


def _best_email(domain: str, emails: list[str]) -> str:
    ranked = sorted(
        emails,
        key=lambda email: (
            0 if _domain_email_match(domain, email) else 1,
            1 if any(free in email.lower() for free in ("gmail.com", "yahoo.com", "hotmail.com", "outlook.com")) else 0,
            len(email),
        ),
    )
    return ranked[0] if ranked else ""


def _is_bad_candidate(track: str, url: str, title: str, snippet: str, summary: str, email: str) -> bool:
    domain = _domain(url)
    if domain in BLOCKED_DOMAINS:
        return True
    haystack = " ".join([url, title, snippet, summary]).lower()
    if any(term in haystack for term in TRACK_BLOCKLIST_TERMS[track]):
        return True
    if not email or any(skip in email.lower() for skip in BLOCKED_EMAIL_PATTERNS):
        return True
    if email.lower().endswith((".png", ".jpg", ".jpeg", ".svg", ".webp", ".gif")):
        return True
    if track == "principal_broker":
        if "broker" not in haystack and "realty" not in haystack and "brokerage" not in haystack:
            return True
        if "property management" in haystack and "brokerage" not in haystack:
            return True
    return False


def _clean_summary(summary: str, limit: int = 110) -> str:
    summary = _clean_text(summary).replace("’", "'").replace("“", '"').replace("”", '"')
    if len(summary) <= limit:
        return summary.rstrip(".")
    trimmed = summary[:limit].rsplit(" ", 1)[0].rstrip(" ,;:-")
    return trimmed


def _score_candidate(track: str, title: str, snippet: str, summary: str, url: str) -> tuple[int, str]:
    haystack = " ".join([title, snippet, summary, url]).lower()
    score = 0
    reasons: list[str] = []
    for keyword in TRACK_KEYWORDS[track]:
        if keyword.lower() in haystack:
            score += 12
            reasons.append(keyword)
    if "tn" in haystack or "tennessee" in haystack:
        score += 8
    if "nashville" in haystack or "middle tennessee" in haystack:
        score += 8
    if "principal broker" in haystack or "managing broker" in haystack:
        score += 16
    if "auction company" in haystack or "auctioneer" in haystack:
        score += 10
    if urlparse(url).netloc.lower().endswith(".gov"):
        score -= 12
    return score, ", ".join(reasons[:4]) or "regional fit"


def _personalized_line(track: str, org: str, city: str, summary: str) -> str:
    location = f" in {city}" if city else ""
    clean_summary = _clean_summary(summary, 95) or "regional execution work"
    if track == "auction_partner":
        return (
            f"I came across {org}{location} while mapping active real-estate auction operators, "
            f"and your positioning around {clean_summary} looks directionally aligned with what we are building."
        )
    return (
        f"I came across {org}{location} while looking for Tennessee broker leadership teams, "
        f"and your platform appears active in {clean_summary or 'brokerage operations and supervision'}."
    )


def _build_email(track: str, org: str, personalized_line: str) -> tuple[str, str]:
    if track == "auction_partner":
        subject = "Controlled Upstream Distress Opportunity Flow"
        body = (
            f"Hi {org} team,\n\n"
            f"{personalized_line}\n\n"
            "I'm reaching out because I'm building a focused distress-origination platform in Middle Tennessee and think there may be a fit with your auction activity.\n\n"
            "We're sourcing and packaging upstream distressed opportunities before they become crowded, then routing only a small number of cleaner, controlled opportunities through a gated partner workflow. This is not broad public inventory or generic lead volume. The focus is curated, auction-viable opportunities with tighter packaging and controlled distribution.\n\n"
            "I'd be interested in a short conversation to understand what kinds of distressed opportunities are most useful to you, how you prefer to evaluate incoming inventory, and whether there's a fit to route a small number of clean opportunities your way.\n\n"
            "If it makes sense, I can share a sample packet and walk you through how we're structuring the flow.\n\n"
            "Best,\n"
            "Patrick Yuri Armour\n"
            "FALCO\n"
            "https://falco.llc\n"
        )
        return subject, body

    subject = "Peregrine Realty Group / Principal Broker Opportunity"
    body = (
        f"Hi {org} team,\n\n"
        f"{personalized_line}\n\n"
        "I'm building Peregrine Realty Group around a live distress-origination and controlled deal-flow system, and I'm looking for the right principal broker relationship to structure the brokerage side correctly from the beginning.\n\n"
        "The underlying origination engine and partner-facing distribution layer already exist. What I'm looking for now is the right principal broker and office home to serve as the appointed principal broker, host the brokerage under an established office/location, oversee brokerage compliance and supervision as required, and help ensure the operation is structured correctly from a regulatory and operational standpoint.\n\n"
        "This is not meant to be a passive license arrangement. I want the brokerage built on a real supervisory and compliance foundation.\n\n"
        "On economics, I'm open to structuring it the right way for the right fit, but the basic framework would be principal broker oversight and hosting, plus revenue share tied to brokerage-side deal flow generated through Peregrine.\n\n"
        "If this is something you'd be open to discussing, I'd value a short conversation.\n\n"
        "Best,\n"
        "Patrick Yuri Armour\n"
        "FALCO\n"
        "https://falco.llc\n"
    )
    return subject, body


def build_candidates(track: str, limit: int = 50) -> list[Candidate]:
    by_domain: dict[str, Candidate] = {}
    rank = 0
    sources: list[dict[str, str]] = []
    sources.extend(_load_seed_rows(track))
    for query in TRACK_QUERIES[track]:
        sources.extend(_search(query, limit=15))

    for result in sources:
            domain = _domain(result["url"])
            if not domain or domain in by_domain:
                continue
            ctx = _page_context(result["url"])
            emails = list(ctx.get("emails") or [])
            primary_email = _best_email(domain, emails)
            if not primary_email:
                continue
            if _is_bad_candidate(track, result["url"], result["title"], result["snippet"], str(ctx.get("summary") or ""), primary_email):
                continue
            score, reason = _score_candidate(track, result["title"], result["snippet"], str(ctx.get("summary") or ""), result["url"])
            personalized = _personalized_line(track, str(ctx["organization"]), str(ctx["city"]), str(ctx["summary"]))
            subject, body = _build_email(track, str(ctx["organization"]), personalized)
            rank += 1
            by_domain[domain] = Candidate(
                track=track,
                rank=rank,
                score=score,
                organization=str(ctx["organization"]),
                contact_name="",
                email=primary_email,
                website=result["url"],
                domain=domain,
                city=str(ctx["city"]),
                state=str(ctx["state"]),
                reason=reason,
                snippet=result["snippet"],
                personalized_line=personalized,
                subject=subject,
                body=body,
                query=result["query"],
            )
    candidates = sorted(by_domain.values(), key=lambda c: (-c.score, c.organization.lower()))
    for idx, candidate in enumerate(candidates, 1):
        candidate.rank = idx
    return candidates[:limit]


def _write_outputs(track: str, candidates: list[Candidate], out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = out_dir / f"{track}_{stamp}"
    json_path = base.with_suffix(".json")
    csv_path = base.with_suffix(".csv")
    md_path = base.with_suffix(".md")

    json_path.write_text(json.dumps([asdict(c) for c in candidates], indent=2), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "rank",
                "score",
                "organization",
                "email",
                "website",
                "city",
                "state",
                "reason",
                "query",
                "subject",
                "personalized_line",
            ],
        )
        writer.writeheader()
        for candidate in candidates:
            writer.writerow(
                {
                    "rank": candidate.rank,
                    "score": candidate.score,
                    "organization": candidate.organization,
                    "email": candidate.email,
                    "website": candidate.website,
                    "city": candidate.city,
                    "state": candidate.state,
                    "reason": candidate.reason,
                    "query": candidate.query,
                    "subject": candidate.subject,
                    "personalized_line": candidate.personalized_line,
                }
            )

    with md_path.open("w", encoding="utf-8") as f:
        f.write(f"# {track}\n\n")
        for candidate in candidates:
            f.write(f"## {candidate.rank}. {candidate.organization}\n")
            f.write(f"- Score: {candidate.score}\n")
            f.write(f"- Email: {candidate.email}\n")
            f.write(f"- Website: {candidate.website}\n")
            f.write(f"- Reason: {candidate.reason}\n")
            f.write(f"- Query: {candidate.query}\n")
            f.write(f"- Personalized line: {candidate.personalized_line}\n")
            f.write(f"- Subject: {candidate.subject}\n\n")
            f.write("```text\n")
            f.write(candidate.body)
            f.write("\n```\n\n")

    return {"json": str(json_path), "csv": str(csv_path), "md": str(md_path)}


def _send_gmail(candidates: Iterable[Candidate], sender: str, app_password: str, max_send: int) -> dict[str, int]:
    context = ssl.create_default_context()
    sent = 0
    errors = 0
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
        server.starttls(context=context)
        server.login(sender, app_password)
        for candidate in list(candidates)[:max_send]:
            msg = EmailMessage()
            msg["From"] = sender
            msg["To"] = candidate.email
            msg["Subject"] = candidate.subject
            msg.set_content(candidate.body)
            try:
                server.send_message(msg)
                sent += 1
            except Exception:
                errors += 1
    return {"sent": sent, "errors": errors}


def main() -> None:
    parser = argparse.ArgumentParser(description="Falco outreach candidate agent")
    parser.add_argument("--track", choices=["auction_partner", "principal_broker", "both"], default="both")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--max-send", type=int, default=10)
    parser.add_argument("--out-dir", default=str(Path("out") / "outreach"))
    args = parser.parse_args()

    tracks = ["auction_partner", "principal_broker"] if args.track == "both" else [args.track]
    summary: dict[str, object] = {}

    gmail_user = os.environ.get("FALCO_GMAIL_USER", "").strip()
    gmail_password = os.environ.get("FALCO_GMAIL_APP_PASSWORD", "").strip()

    for track in tracks:
        candidates = build_candidates(track, args.limit)
        outputs = _write_outputs(track, candidates, Path(args.out_dir))
        send_result = {"sent": 0, "errors": 0, "enabled": False}
        if args.send and gmail_user and gmail_password:
            send_result = _send_gmail(candidates, gmail_user, gmail_password, args.max_send)
            send_result["enabled"] = True
        summary[track] = {
            "candidate_count": len(candidates),
            "outputs": outputs,
            "send": send_result,
        }

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
