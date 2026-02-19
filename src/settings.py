import os

def clip_raw_snippet(text: str, max_chars: int | None = None) -> str:
    """
    Hard-cap raw notice text so Notion doesn't get flooded.
    Default cap can be set via env var FALCO_MAX_RAW_SNIPPET_CHARS.
    """
    if text is None:
        return ""

    s = str(text).strip()
    if not s:
        return ""

    # Prefer explicit arg, else env var, else safe default
    if max_chars is None:
        try:
            max_chars = int(os.getenv("FALCO_MAX_RAW_SNIPPET_CHARS", "1200"))
        except Exception:
            max_chars = 1200

    if max_chars <= 0:
        return ""

    # normalize whitespace a bit
    s = " ".join(s.split())

    if len(s) <= max_chars:
        return s

    return s[: max_chars - 3].rstrip() + "..."
