"""Provider contracts; no provider names or production credentials."""

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from marketplace_pipeline.adapters.http import HttpSession


@dataclass(frozen=True)
class ProviderCapabilities:
    authentication: str
    pagination: str
    max_page_size: int
    max_pages: int
    max_results: int
    marketplaces: tuple[str, ...]
    source_schema_version: str
    entity_kinds: tuple[str, ...] = ("listing",)
    rate_limit: str = "Account-specific; verify private quota before acquisition"
    retention: str = "Account agreement; explicit retention_days, no automatic deletion"


@dataclass(frozen=True)
class ProviderPolicy:
    categories: tuple[str, ...] = ("gpu", "ram", "ssd", "keyboard", "mouse", "monitor")
    minimum_interval: float = 1.0
    retries: int = 3
    default_currency: str | None = None
    currency_marketplace: str | None = None
    required_currency: str | None = None
    live: bool = True


@runtime_checkable
class SourceAdapter(Protocol):
    source_id: str
    endpoint_version: str
    capabilities: ProviderCapabilities
    supported_entity_kinds: tuple[str, ...]
    session: HttpSession

    def fetch(self, query: dict, page: int) -> bytes: ...
    def parse(self, body: bytes, query: dict, page: int) -> tuple[list[dict], bool]: ...
    def diagnostics(self) -> dict: ...
    def classify_error(self, error: Exception) -> str: ...


class AdapterDiagnostics:
    supported_entity_kinds = ("listing",)

    def diagnostics(self) -> dict:
        return {}

    def classify_error(self, error: Exception) -> str:
        from marketplace_pipeline.adapters.http import ContractError

        return str(error) if isinstance(error, ContractError) else "acquisition_interrupted"
