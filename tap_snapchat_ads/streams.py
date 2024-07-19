"""Stream type classes for tap-snapchat-ads."""

from __future__ import annotations

import sys
import typing as t

import pendulum
from datetime import datetime, timedelta
from dateutil.parser import parse
from requests import Response
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator, TPageToken

from tap_snapchat_ads.client import SnapchatAdsStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

from typing import Any, Optional

from tap_snapchat_ads.fields import ALL_STAT_FIELDS

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class DayPaginator(BaseAPIPaginator):
    """Day paginator class."""

    def __init__(
        self,
        start_value: Any,  # noqa: ANN401
        json_key_array: str,
        json_key_record: str,
    ) -> None:
        """Initialize the paginator with a json_key to access the payload."""
        super().__init__(start_value)
        self.json_key_array = json_key_array
        self.json_key_record = json_key_record

    def get_next(self, response: Response) -> TPageToken | None:
        """Return the next page URL."""
        return response.json()[self.json_key_array][0][self.json_key_record]["end_time"]

    def has_more(self, response: Response) -> bool:
        """Return True if there are more records."""
        end_time = response.json()[self.json_key_array][0][self.json_key_record][
            "end_time"
        ]
        tz = pendulum.timezone("UTC")
        return not timedelta(days=1) < datetime.now(tz=tz) - parse(end_time)


class OrganizationsStream(SnapchatAdsStream):
    """Define custom stream."""

    name = "organizations"
    json_key_array = "organizations"
    json_key_record = "organization"
    path = "/me/organizations"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "organizations.json"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, Any]]:
        """Return a generator of records."""
        all_organizations = super().get_records(context)
        selected_org_ids = self.config.get("organization_ids", [])
        if len(selected_org_ids) == 0:
            yield from all_organizations

        for org in all_organizations:
            if org["id"] in selected_org_ids:
                yield org

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            "_sdc_org_id": record["id"],
        }


class FundingSourcesStream(SnapchatAdsStream):
    """Define custom stream."""

    name = "funding_sources"
    json_key_array = "fundingsources"
    json_key_record = "fundingsource"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/fundingsources"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "funding_sources.json"


class BillingCentersStream(SnapchatAdsStream):
    """Define billing centers stream."""

    name = "billing_centers"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/billingcenters"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "billing_centers.json"


class MembersStream(SnapchatAdsStream):
    """Define members stream."""

    name = "members"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/members"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "members.json"


class RolesStream(SnapchatAdsStream):
    """Define roles stream."""

    name = "roles"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/roles"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "roles.json"


class AdAccountsStream(SnapchatAdsStream):
    """Define ad accounts stream."""

    name = "ad_accounts"
    json_key_array = "adaccounts"
    json_key_record = "adaccount"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/adaccounts"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "ad_accounts.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            "_sdc_timezone": record["timezone"],
            "_sdc_adaccount_id": record["id"],
        }

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, Any]]:
        """Return a generator of records."""
        all_adaccounts = super().get_records(context)
        selected_adaccount_ids = self.config.get("adaccount_ids", [])
        if len(selected_adaccount_ids) == 0:
            yield from all_adaccounts

        for adaccount in all_adaccounts:
            if adaccount["id"] in selected_adaccount_ids:
                yield adaccount


class AdAccountsStatsDailyStream(SnapchatAdsStream):
    """Define ad accounts stats daily stream."""

    name = "ad_account_stats_stats"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/ad_account_stats.json"
    date_window_size = 31

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Return a new paginator instance."""
        start_date = self.config.get("start_date")
        return DayPaginator(start_date, self.json_key_array, self.json_key_record)

    def get_url_params(
        self, context: dict | None, next_page_token: str | None
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        start_time = next_page_token
        start_time = parse(start_time)
        tz = pendulum.timezone(context["_sdc_timezone"])
        start_time = start_time.astimezone(tz=tz)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = start_time.astimezone(pendulum.timezone("UTC"))

        end_time = datetime.now(tz=tz) - timedelta(days=1)
        min_timeframe = timedelta(days=1)
        if (end_time - start_time).days < min_timeframe.days:
            return None

        # If greater than 32 days, set to 31 days
        max_timeframe = timedelta(days=31)
        if (end_time - start_time).days > max_timeframe.days:
            end_time = start_time + max_timeframe

        end_time = end_time.astimezone(tz=tz)
        end_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = end_time.astimezone(pendulum.timezone("UTC"))

        start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "start_time": start_time,
            "end_time": end_time,
            "fields": "spend",
            "granularity": "DAY",
            "omit_empty": False,
            "conversion_source_types": "web,app,total",
            "swipe_up_attribution_window": self.config.get(
                "swipe_up_attribution_window", "28_DAY"
            ),
            "view_attribution_window": self.config.get(
                "view_attribution_window", "1_DAY"
            ),
        }

    def parse_response(self, response: Response) -> t.Iterable[dict[str, Any]]:
        """Parse the response and return an iterator of result records."""
        res = super().parse_response(response)
        # Each 'row' has upto a month worth of data, so we need to break it down
        # Unpack the timeseries object, contains both start_date, end_date and stats
        for row in res:
            for stat in row["stats"]:
                yield {
                    "id": row["id"],
                    "start_time": stat["start_time"],
                    "end_time": stat["end_time"],
                    "spend": stat["spend"],
                }
        # TODO: Implement the response parsing logic here