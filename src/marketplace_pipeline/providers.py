"""Registry and built-in composition root; onboarding adds registrations here."""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from marketplace_pipeline.adapters.contracts import (
    ProviderCapabilities,
    ProviderPolicy,
    SourceAdapter,
)
from marketplace_pipeline.adapters.production import Credentials, EbayAdapter, RakutenAdapter
from marketplace_pipeline.mapping import FieldRule, MappingSpec


@dataclass(frozen=True)
class Provider:
    source_id: str
    capabilities: ProviderCapabilities
    policy: ProviderPolicy
    factory: Callable
    marketplace: Callable[[Mapping[str, str]], str]
    mappings: tuple[MappingSpec, ...]
    current_mapping: str

    def mapping(self, version=None):
        selected = version or self.current_mapping
        for spec in self.mappings:
            if spec.version == selected:
                return spec
        raise ValueError("Unsupported mapping version")


class ProviderRegistry:
    def __init__(self):
        self._providers: dict[str, Provider] = {}

    def register(self, provider: Provider):
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", provider.source_id):
            raise ValueError("Invalid provider ID")
        if provider.source_id in self._providers:
            raise ValueError("Provider already registered")
        provider.mapping()
        self._providers[provider.source_id] = provider

    def get(self, source: str) -> Provider:
        try:
            return self._providers[source]
        except KeyError:
            raise ValueError("Unregistered provider") from None

    def names(self, *, live_only=False):
        return tuple(
            p.source_id for p in self._providers.values() if not live_only or p.policy.live
        )


def credentials(env, identity, secret):
    result = Credentials(env.get(identity, ""), env.get(secret, ""))
    if not result.client_id or not result.secret:
        raise ValueError("Missing provider credentials")
    return result


def ebay_factory(session, env) -> SourceAdapter:
    return EbayAdapter(session, credentials(env, "EBAY_CLIENT_ID", "EBAY_CLIENT_SECRET"))


def rakuten_factory(session, env) -> SourceAdapter:
    return RakutenAdapter(
        session,
        credentials(env, "RAKUTEN_APPLICATION_ID", "RAKUTEN_ACCESS_KEY"),
        origin=env.get("RAKUTEN_ORIGIN", ""),
        referer=env.get("RAKUTEN_REFERER", ""),
    )


def listing_rules(identity, title, amount, currency, version):
    return tuple(
        FieldRule(
            target,
            (path,),
            "text"
            if version != "legacy-1" and target in {"brand", "model", "variant"}
            else "identity",
        )
        for target, path in (
            ("identity", identity),
            ("title", title),
            ("amount", amount),
            ("currency", currency),
            ("brand", "brand"),
            ("model", "model"),
            ("variant", "variant"),
        )
    )


registry = ProviderRegistry()
registry.register(
    Provider(
        "ebay",
        EbayAdapter.capabilities,
        ProviderPolicy(),
        ebay_factory,
        lambda env: env.get("EBAY_MARKETPLACE_ID", "EBAY_US"),
        tuple(
            MappingSpec(
                v,
                listing_rules("itemId", "title", "price.value", "price.currency", v),
                ("browse-v1",),
            )
            for v in ("legacy-1", "2")
        ),
        "2",
    )
)
registry.register(
    Provider(
        "rakuten",
        RakutenAdapter.capabilities,
        ProviderPolicy(
            default_currency="JPY", currency_marketplace="RAKUTEN_JP", required_currency="JPY"
        ),
        rakuten_factory,
        lambda env: "RAKUTEN_JP",
        tuple(
            MappingSpec(
                v,
                listing_rules("itemCode", "itemName", "itemPrice", "currency", v),
                ("ichiba-20260701",),
            )
            for v in ("legacy-1", "2")
        ),
        "2",
    )
)
