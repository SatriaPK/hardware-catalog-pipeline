"""Third-source experiment. SIMULATION ONLY; always uses an offline transport."""

import json
from dataclasses import replace
from decimal import Decimal

import httpx

from marketplace_pipeline.adapters.contracts import (
    AdapterDiagnostics,
    ProviderCapabilities,
    ProviderPolicy,
)
from marketplace_pipeline.adapters.http import ContractError, HttpSession, Policy
from marketplace_pipeline.mapping import FieldRule, MappingSpec
from marketplace_pipeline.providers import Provider


class SimulatedAdapter(AdapterDiagnostics):
    source_id = "simulated"
    endpoint_version = "simulation-1"
    capabilities = ProviderCapabilities(
        "none",
        "page",
        50,
        10,
        500,
        ("SIMULATION",),
        "simulation-1",
        rate_limit="Offline only",
        retention="Synthetic data",
    )

    def __init__(self, pages: list[dict], *, version="simulation-1"):
        self.endpoint_version = version
        self.capabilities = replace(self.capabilities, source_schema_version=version)
        self.pages = pages
        self.session = HttpSession(
            httpx.MockTransport(self.respond),
            Policy(request_budget=1000, max_retries=0),
            sleep=lambda _: None,
        )

    def respond(self, request):
        page = int(request.url.params["page"])
        return httpx.Response(200, json=self.pages[page - 1])

    def fetch(self, query, page):
        if not 1 <= page <= len(self.pages) or not 1 <= query["page_size"] <= 50:
            raise ContractError("invalid_page")
        return self.session.request(
            "GET", "https://simulation.invalid/catalog", params={"page": page}
        ).content

    def parse(self, body, query, page):
        try:
            data = json.loads(body, parse_float=Decimal)
            rows = data["catalog"]["offers"]
            if data["version"] != self.endpoint_version:
                raise ValueError
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise ValueError
            if type(data["continuation"]) is not bool:
                raise ValueError
            return rows, data["continuation"]
        except (ValueError, TypeError, KeyError):
            raise ContractError("schema_drift") from None


def definition():
    def offline_only(session, env):
        raise ValueError("Supply synthetic pages to SimulatedAdapter explicitly")

    rules = (
        FieldRule("identity", ("identity.sku",), "text", True),
        FieldRule("title", ("description.label", "description.name"), "text", True),
        FieldRule("amount", ("offer.money.amount", "offer.cost"), "decimal", True),
        FieldRule("currency", ("offer.money.code", "offer.currency"), "text", True),
        FieldRule("brand", ("manufacturer.name",), "text"),
        FieldRule("model", ("manufacturer.model",), "text"),
        FieldRule("variant", ("variant",), "text"),
    )
    return Provider(
        "simulated",
        SimulatedAdapter.capabilities,
        ProviderPolicy(live=False),
        offline_only,
        lambda env: "SIMULATION",
        (
            MappingSpec("1", rules, ("simulation-1",)),
            MappingSpec("2", rules, ("simulation-1", "simulation-2")),
        ),
        "2",
    )
