"""Official HTTP adapters. The same classes run with MockTransport in tests."""

import json
from dataclasses import dataclass, field
from decimal import Decimal

import httpx

from marketplace_pipeline.adapters.http import ContractError, HttpSession, envelope
from marketplace_pipeline.adapters.rakuten_diagnostics import (
    response_structure,
    safe_error,
    website_headers,
)

EBAY_SEARCH = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_TOKEN = "https://api.ebay.com/identity/v1/oauth2/token"
RAKUTEN_SEARCH = "https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20260701"


@dataclass(frozen=True)
class Credentials:
    client_id: str = field(repr=False)
    secret: str = field(repr=False)


class EbayAdapter:
    source_id = "ebay"
    endpoint_version = "browse-v1"

    def __init__(self, session: HttpSession, credentials: Credentials):
        self.session, self.credentials = session, credentials
        self._token = ""
        self._expires = 0.0

    def fetch(self, query: dict, page: int) -> bytes:
        if not 1 <= query["page_size"] <= 200 or (page - 1) * query["page_size"] >= 10000:
            raise ContractError("invalid_page")
        if not self._token or self.session.clock() >= self._expires:
            response = self.session.request(
                "POST",
                EBAY_TOKEN,
                auth=(self.credentials.client_id, self.credentials.secret),
                data={
                    "grant_type": "client_credentials",
                    "scope": "https://api.ebay.com/oauth/api_scope",
                },
            )
            token = envelope(response.content)
            if (
                not isinstance(token.get("access_token"), str)
                or not token["access_token"]
                or type(token.get("expires_in")) is not int
                or token["expires_in"] <= 0
            ):
                raise ContractError("invalid_token_response")
            self._token = token["access_token"]
            self._expires = self.session.clock() + max(0, token["expires_in"] - 30)
        return self.session.request(
            "GET",
            EBAY_SEARCH,
            headers={
                "Authorization": f"Bearer {self._token}",
                "X-EBAY-C-MARKETPLACE-ID": query["marketplace_id"],
                "Accept-Language": query["locale"],
            },
            params={
                "q": query["query"],
                "limit": query["page_size"],
                "offset": (page - 1) * query["page_size"],
            },
        ).content

    def parse(self, body: bytes, query: dict, page: int) -> tuple[list[dict], bool]:
        data, rows = parsed(body, "itemSummaries", allow_zero_total=True)
        more = bool(data.get("next")) and bool(rows)
        if more:
            try:
                url = httpx.URL(data["next"])
                if (
                    url.scheme != "https"
                    or url.host != "api.ebay.com"
                    or url.userinfo
                    or url.port not in {None, 443}
                    or url.path != httpx.URL(EBAY_SEARCH).path
                    or url.params.get_list("offset") != [str(page * query["page_size"])]
                    or url.params.get_list("limit") != [str(query["page_size"])]
                ):
                    raise ValueError
            except (ValueError, TypeError, httpx.InvalidURL):
                raise ContractError("invalid_next_cursor") from None
        return rows, more and page * query["page_size"] < 10000


class RakutenAdapter:
    source_id = "rakuten"
    endpoint_version = "ichiba-20260701"

    def __init__(
        self, session: HttpSession, credentials: Credentials, *, origin: str = "", referer: str = ""
    ):
        self.session, self.credentials = session, credentials
        self.website_headers = website_headers(origin, referer)
        self.provider_error: dict | None = None
        self.response_structure: dict | None = None
        self.schema_violations: list[str] = []
        self.session.client.event_hooks["response"].append(self._capture_error)

    def _capture_error(self, response: httpx.Response) -> None:
        if response.status_code != 200:
            self.provider_error = safe_error(
                response, (self.credentials.client_id, self.credentials.secret)
            )

    def fetch(self, query: dict, page: int) -> bytes:
        self.provider_error = None
        if not 1 <= query["page_size"] <= 30 or not 1 <= page <= 100:
            raise ContractError("invalid_page")
        return self.session.request(
            "GET",
            RAKUTEN_SEARCH,
            headers={"accessKey": self.credentials.secret, **self.website_headers},
            params={
                "applicationId": self.credentials.client_id,
                "keyword": query["query"],
                "format": "json",
                "formatVersion": 2,
                "hits": query["page_size"],
                "page": page,
            },
        ).content

    def parse(self, body: bytes, query: dict, page: int) -> tuple[list[dict], bool]:
        self.schema_violations = []
        self.response_structure = None
        try:
            data = json.loads(body, parse_float=Decimal)
        except (ValueError, UnicodeError):
            self.schema_violations = ["response.invalid_json"]
            raise ContractError("schema_drift") from None
        self.response_structure = response_structure(
            data, (self.credentials.client_id, self.credentials.secret)
        )
        if not isinstance(data, dict):
            self.schema_violations = ["response.expected_object"]
            raise ContractError("schema_drift")
        if data.get("errors") or data.get("error"):
            self.schema_violations.append("response.error_envelope")
        # Public examples use items; the user's 20260701 response uses Items.
        # Reject ambiguity rather than selecting one payload silently.
        if "items" in data and "Items" in data:
            self.schema_violations.append("items.ambiguous_keys")
        rows = data.get("items") if "items" in data else data.get("Items")
        if not isinstance(rows, list):
            self.schema_violations.append("items.expected_array")
        elif any(not isinstance(row, dict) for row in rows):
            self.schema_violations.append("items.entry_expected_object")
        elif any("item" in row or "Item" in row for row in rows):
            self.schema_violations.append("items.unexpected_format_v1_wrapper")
        if type(data.get("page")) is not int:
            self.schema_violations.append("page.expected_integer")
        elif data["page"] != page:
            self.schema_violations.append("page.request_mismatch")
        if type(data.get("pageCount")) is not int:
            self.schema_violations.append("pageCount.expected_integer")
        elif not 0 <= data["pageCount"] <= 100:
            self.schema_violations.append("pageCount.out_of_documented_range")
        if self.schema_violations:
            raise ContractError("schema_drift")
        return rows, bool(rows) and page < data["pageCount"] and page < 100


def parsed(body: bytes, key: str, *, allow_zero_total: bool = False) -> tuple[dict, list[dict]]:
    try:
        data = json.loads(body, parse_float=Decimal)
        if not isinstance(data, dict) or data.get("errors") or data.get("error"):
            raise ValueError
        rows = data.get(key)
        if (
            rows is None
            and allow_zero_total
            and type(data.get("total")) is int
            and data["total"] == 0
        ):
            rows = []
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError
        return data, rows
    except (ValueError, TypeError, UnicodeError):
        raise ContractError("schema_drift") from None
