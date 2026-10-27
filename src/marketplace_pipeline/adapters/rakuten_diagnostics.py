"""Bounded provider error evidence; never export request headers or raw bodies."""

import base64
import json
import re
from urllib.parse import quote, quote_plus, urlsplit

import httpx


def response_structure(data, secrets: tuple[str, ...]) -> dict:
    """Schema names/types only; never serialize product values or entire objects."""

    def kind(value):
        return {
            dict: "object",
            list: "array",
            str: "string",
            int: "integer",
            float: "number",
            bool: "boolean",
            type(None): "null",
        }.get(type(value), "number")

    def names(value):
        if not isinstance(value, dict):
            return []
        return sorted(
            key
            for key in value
            if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", key)
            and not any(secret and secret in key for secret in secrets)
            and not re.search(r"(?i)token|secret|accesskey|applicationid|authorization", key)
        )[:100]

    keys = [key for key in ("items", "Items") if isinstance(data, dict) and key in data]
    items_key = keys[0] if len(keys) == 1 else None
    items = data[items_key] if items_key else None
    first = items[0] if isinstance(items, list) and items else None
    return {
        "top_level_type": kind(data),
        "top_level_keys": names(data),
        "items_key": items_key,
        "items_type": kind(items) if items_key else "ambiguous" if keys else "missing",
        "first_item_type": kind(first) if isinstance(items, list) and items else "absent",
        "first_item_fields": names(first),
        "pagination_field_types": {
            field: kind(data[field]) if isinstance(data, dict) and field in data else "missing"
            for field in ("page", "pageCount")
        },
    }


def website_headers(origin: str = "", referer: str = "") -> dict[str, str]:
    headers = {}
    for name, value in (("Origin", origin), ("Referer", referer)):
        if not value:
            continue
        try:
            url = urlsplit(value)
            if (
                not value.isascii()
                or any(ord(c) <= 32 or ord(c) == 127 for c in value)
                or url.scheme not in {"http", "https"}
                or not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
                or (name == "Origin" and url.path not in {"", "/"})
            ):
                raise ValueError
            _ = url.port
        except ValueError:
            raise ValueError("Invalid Rakuten website header configuration") from None
        headers[name] = value.rstrip("/") if name == "Origin" else value
    if origin and referer and urlsplit(origin).netloc != urlsplit(referer).netloc:
        raise ValueError("Rakuten Origin and Referer must identify the same website")
    if origin and referer and urlsplit(origin).scheme != urlsplit(referer).scheme:
        raise ValueError("Rakuten Origin and Referer must identify the same website")
    return headers


def safe_error(response: httpx.Response, secrets: tuple[str, ...]) -> dict:
    def clean(value):
        if not isinstance(value, (str, int)) or isinstance(value, bool):
            return None
        text = str(value)
        variants = {
            v
            for secret in secrets
            if secret
            for v in (
                secret,
                quote(secret, safe=""),
                quote_plus(secret),
                json.dumps(secret, ensure_ascii=True)[1:-1],
                base64.b64encode(secret.encode()).decode(),
            )
        }
        for secret in sorted(variants, key=len, reverse=True):
            text = text.replace(secret, "<REDACTED>")
        text = re.sub(r"https?://[^\s<>]+", "<URL_REDACTED>", text)
        text = re.sub(r"(?i)\bBearer\s+\S+", "Bearer <REDACTED>", text)
        text = re.sub(
            r"(?i)\b(accesskey|applicationid|api[_-]?key|token|secret|authorization)\b[\s\"':=]+[^\s,;]+",
            r"\1=<REDACTED>",
            text,
        )
        return " ".join(text.split())[:512]

    response.read()
    if len(response.content) > 65536:
        return {"code": None, "message": None, "body_status": "oversized_omitted"}
    try:
        data = response.json()
    except (ValueError, UnicodeError):
        return {"code": None, "message": None, "body_status": "non_json_omitted"}
    if not isinstance(data, dict):
        return {"code": None, "message": None, "body_status": "unrecognized_json"}
    # Inspect only known envelope/diagnostic fields. Never dump unknown keys or
    # values (which may themselves contain credentials). Bounds prevent recursion
    # through arbitrarily large provider envelopes.
    pending = [(data, 0)]
    fallback = {"code": None, "message": None, "body_status": "unrecognized_json_fields"}
    for _ in range(32):
        if not pending:
            break
        node, depth = pending.pop(0)
        if isinstance(node, list):
            if depth < 6:
                pending.extend((item, depth + 1) for item in node[:4])
            continue
        if not isinstance(node, dict):
            continue
        fields = {re.sub(r"[_-]", "", key).lower(): value for key, value in node.items()}

        def first(names, fields=fields):
            return next(
                (value for name in names if (value := clean(fields.get(name))) is not None), None
            )

        code = first(("code", "errorcode", "error"))
        message = first(("errordescription", "message", "errormessage", "description"))
        if code is not None or message is not None:
            candidate = {"code": code, "message": message, "body_status": "json_fields_only"}
            if code is not None and message is not None:
                return candidate
            if fallback["code"] is None and fallback["message"] is None:
                fallback = candidate
        if depth < 6:
            pending.extend(
                (fields[name], depth + 1)
                for name in ("error", "errors", "response", "data", "result", "fault", "details")
                if name in fields and isinstance(fields[name], (dict, list))
            )
    return fallback
