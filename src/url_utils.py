import math
from typing import Optional


def normalize_url(raw) -> Optional[str]:
    """Normalize a raw URL value to a full https:// URL, or None if invalid."""
    if raw is None:
        return None
    if isinstance(raw, float) and math.isnan(raw):
        return None
    url = str(raw).strip()
    if not url:
        return None
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return "https://" + url
