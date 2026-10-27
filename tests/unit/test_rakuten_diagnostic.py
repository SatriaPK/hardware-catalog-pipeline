"""Keep the provider contract regressions that previously blocked acquisition."""

import json

import httpx
import pytest

from marketplace_pipeline.adapters.http import ContractError, HttpSession, Policy
from marketplace_pipeline.adapters.production import Credentials, RakutenAdapter
from marketplace_pipeline.adapters.rakuten_diagnostics import safe_error, website_headers


@pytest.mark.parametrize("key", ["items", "Items"])
def test_rakuten_actual_adapter_v2(key):
    def respond(request):
        assert request.url.params["formatVersion"] == "2"
        assert request.headers["accessKey"] == "TEST_KEY"
        assert request.headers["Origin"] == "https://catalog.example.org"
        return httpx.Response(
            200,
            json={
                key: [{"itemCode": "test:1", "itemName": "TEST", "itemPrice": 10}],
                "page": 1,
                "pageCount": 1,
            },
        )

    session = HttpSession(httpx.MockTransport(respond), Policy(request_budget=1))
    try:
        adapter = RakutenAdapter(
            session, Credentials("TEST_APP", "TEST_KEY"), origin="https://catalog.example.org"
        )
        query = {"page_size": 10, "query": "SSD"}
        rows, more = adapter.parse(adapter.fetch(query, 1), query, 1)
        assert len(rows) == 1 and not more
        assert adapter.response_structure["items_key"] == key
    finally:
        session.close()


@pytest.mark.parametrize(
    "body",
    [
        {"items": [], "Items": []},
        {"Items": {}},
        {"Items": [{"Item": {}}]},
        {"Items": [], "page": "1"},
        {"Items": [], "pageCount": 101},
    ],
)
def test_rakuten_invalid_contract_not_silently_coerced(body):
    session = HttpSession(httpx.MockTransport(lambda _: httpx.Response(200)))
    try:
        adapter = RakutenAdapter(session, Credentials("TEST_APP", "TEST_KEY"))
        with pytest.raises(ContractError, match="schema_drift"):
            adapter.parse(json.dumps({"page": 1, "pageCount": 1, **body}).encode(), {}, 1)
    finally:
        session.close()


def test_error_redaction_and_header_validation():
    result = safe_error(
        httpx.Response(
            403, json={"error": {"code": "403", "message": "CLIENT_IP_NOT_ALLOWED TEST_SECRET"}}
        ),
        ("TEST_SECRET",),
    )
    assert "TEST_SECRET" not in str(result)
    assert "CLIENT_IP_NOT_ALLOWED" in result["message"]
    with pytest.raises(ValueError):
        website_headers("https://example.org\r\nInjected: yes")
