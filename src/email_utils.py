import re
from typing import List

_EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
_RETINA_SUFFIX = re.compile(r'@\dx$', re.IGNORECASE)

_BLOCKED_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.js', '.css', '.map'}
_BLOCKED_LOCAL_PARTS = {'noreply', 'no-reply', 'donotreply', 'charset', 'keyframes', 'import', 'media'}
_BLOCKED_DOMAINS = {'example.com', 'yourdomain.com', 'domain.com'}


def _is_false_positive(email: str) -> bool:
    local, _, domain = email.lower().partition('@')
    if _RETINA_SUFFIX.search('@' + domain):
        return True
    for ext in _BLOCKED_EXTENSIONS:
        if domain.endswith(ext):
            return True
    if local in _BLOCKED_LOCAL_PARTS:
        return True
    if domain in _BLOCKED_DOMAINS:
        return True
    return False


def extract_emails(html: str) -> List[str]:
    """Extract, filter, and deduplicate emails from HTML content."""
    found = _EMAIL_REGEX.findall(html)
    seen = {}
    for email in found:
        key = email.lower()
        if key not in seen and not _is_false_positive(key):
            seen[key] = key
    return list(seen.values())
