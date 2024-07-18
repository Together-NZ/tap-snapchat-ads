"""Stream type classes for tap-snapchat-ads."""

from __future__ import annotations

import sys
import typing as t

from requests import Response
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_snapchat_ads.client import SnapchatAdsStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

from typing import Any, Optional

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class OrganizationsStream(SnapchatAdsStream):
    """Define custom stream."""

    name = "organizations"
    path = "/me/organizations"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "organizations.json"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, Any]]:
        """Return a generator of records."""
        all_organizations = super().get_records(context)
        selected_org_ids = self.config.get("organization_ids", [])
        if not selected_org_ids:
            return all_organizations

        for org in all_organizations:
            if org["id"] in selected_org_ids:
                yield org

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            "parent_id": record["id"],
        }


class FundingSourcesStream(SnapchatAdsStream):
    """Define custom stream."""

    name = "funding_sources"
    parent_stream_type = OrganizationsStream
    path = "organizations/{parent_id}/fundingsources"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "funding_sources.json"


class BillingCentersStream(SnapchatAdsStream):
    """Define billing centers stream."""

    name = "billing_centers"
    parent_stream_type = OrganizationsStream
    path = "organizations/{parent_id}/billingcenters"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "billing_centers.json"


class MembersStream(SnapchatAdsStream):
    """Define members stream."""

    name = "members"
    parent_stream_type = OrganizationsStream
    path = "organizations/{parent_id}/members"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "members.json"


class RolesStream(SnapchatAdsStream):
    """Define roles stream."""

    name = "roles"
    parent_stream_type = OrganizationsStream
    path = "organizations/{parent_id}/roles"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "roles.json"


class AdAccountsStream(SnapchatAdsStream):
    """Define ad accounts stream."""

    name = "ad_accounts"
    parent_stream_type = OrganizationsStream
    path = "organizations/{parent_id}/adaccounts"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "ad_accounts.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            "grandparent_id": context["parent_id"],
            "parent_id": record["id"],
        }


class AdAccountsStatsDaily(SnapchatAdsStream):
    """Define ad accounts stats daily stream."""

    name = "ad_accounts_stats_daily"
    parent_stream_type = AdAccountsStream
    path = "organizations/{grandparent_id}/adaccounts/{parent_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/ad_account_stats.json"
