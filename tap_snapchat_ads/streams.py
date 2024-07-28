"""Stream type classes for tap-snapchat-ads."""

from __future__ import annotations

import sys
import typing as t

from dateutil.parser import parse

from tap_snapchat_ads.client import SnapchatAdsStream, SnapchatStatsStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

from typing import Any

from tap_snapchat_ads.fields import ALL_STAT_FIELDS

SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"


def remove_invalid_hourly_metrics(metrics: list[str]) -> list[str]:
    """Remove invalid hourly metrics."""
    return [
        metric
        for metric in metrics
        if metric
        not in ["attachment_frequency", "attachment_uniques", "frequency", "uniques"]
    ]


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

    def get_child_context(
        self, record: dict, context: dict | None
    ) -> dict | None:  # noqa: ARG002
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
    json_key_array = "billingcenters"
    json_key_record = "billingcenter"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/billingcenters"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "billing_centers.json"


class MembersStream(SnapchatAdsStream):
    """Define members stream."""

    name = "members"
    json_key_array = "members"
    json_key_record = "member"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/members"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "members.json"


class RolesStream(SnapchatAdsStream):
    """Define roles stream."""

    name = "roles"
    json_key_array = "roles"
    json_key_record = "role"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/roles"
    primary_keys: t.ClassVar[list[str]] = ["id"]
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


class AdAccountsStatsDailyStream(SnapchatStatsStream):
    """Define ad accounts stats daily stream."""

    name = "ad_account_daily_stats"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/ad_account_stats.json"

    date_window_size = 30
    fields = "spend"
    granularity = "DAY"


class AdAccountsStatsHourlyStream(SnapchatStatsStream):
    """Define ad accounts stats daily stream."""

    name = "ad_account_hourly_stats"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/ad_account_stats.json"

    date_window_size = 7
    fields = "spend"
    granularity = "HOUR"


class AudienceSegementsStream(SnapchatAdsStream):
    """Define audience segments stream."""

    name = "audience_segments"
    json_key_array = "segments"
    json_key_record = "segment"
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/segments"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "audience_segments.json"


class PixelsStream(SnapchatAdsStream):
    """Define pixels stream."""

    name = "pixels"
    json_key_array = "pixels"
    json_key_record = "pixel"
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/pixels"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "pixels.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            "_sdc_timezone": context["_sdc_timezone"],
            "_sdc_pixel_id": record["id"],
        }


class PixelDomainStatsStream(SnapchatAdsStream):
    """Define pixel domain stats stream."""

    name = "pixel_domain_stats"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = PixelsStream
    path = "/pixels/{_sdc_pixel_id}/domains/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "pixel_domain_stats.json"


class MediaStream(SnapchatAdsStream):
    """Define media stream."""

    name = "media"
    json_key_array = "media"
    json_key_record = "media"
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/media"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "media.json"


class CreativesStream(SnapchatAdsStream):
    """Define creatives stream."""

    name = "creatives"
    json_key_array = "creatives"
    json_key_record = "creative"
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/creatives"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "creatives.json"


class PhoneNumbersStream(SnapchatAdsStream):
    """Define phone numbers stream."""

    name = "phone_numbers"
    json_key_array = "phone_numbers"
    json_key_record = "phone_number"
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/phone_numbers"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "phone_numbers.json"


class CampaignsStream(SnapchatAdsStream):
    """Define campaigns stream."""

    name = "campaigns"
    json_key_array = "campaigns"
    json_key_record = "campaign"
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/campaigns"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "campaigns.json"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, Any]]:
        """Return a generator of records."""
        all_campaigns = super().get_records(context)
        contextual_start_time = self.get_starting_timestamp(context)

        for campaign in all_campaigns:
            # Don't sync campaigns that do not fall within the date range
            end_time = parse(campaign["end_time"]) if campaign.get("end_time") else None
            if end_time and end_time < contextual_start_time:
                continue
            yield campaign

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        start_time = record.get("start_time")
        end_time = record.get("end_time")
        start_time = parse(start_time)
        end_time = parse(end_time) if end_time else None
        return {
            "_sdc_timezone": context["_sdc_timezone"],
            "_sdc_start_time": start_time,
            "_sdc_end_time": end_time,
            "_sdc_campaign_id": record["id"],
        }


class CampaignStatsDailyStream(SnapchatStatsStream):
    """Define campaign stats daily stream."""

    name = "campaign_stats_daily"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = CampaignsStream
    path = "/campaigns/{_sdc_campaign_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/stats.json"

    date_window_size = 30
    fields = ",".join(ALL_STAT_FIELDS)
    granularity = "DAY"


class CampaignStatsHourlyStream(SnapchatStatsStream):
    """Define campaign stats hourly stream."""

    name = "campaign_stats_hourly"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = CampaignsStream
    path = "/campaigns/{_sdc_campaign_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/stats.json"

    date_window_size = 7
    fields = ",".join(remove_invalid_hourly_metrics(ALL_STAT_FIELDS))
    granularity = "HOUR"


class AdSquadsStream(SnapchatAdsStream):
    """Define ad squads stream."""

    name = "ad_squads"
    json_key_array = "adsquads"
    json_key_record = "adsquad"
    parent_stream_type = CampaignsStream
    path = "/campaigns/{_sdc_campaign_id}/adsquads"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "ad_squads.json"
    
    def get_records(self, context: dict | None) -> t.Iterable[dict[str, Any]]:
        """Return a generator of records."""
        all_adsquads = super().get_records(context)
        contextual_start_time = self.get_starting_timestamp(context)
    
        for ad_squad in all_adsquads:
            # Don't sync campaigns that do not fall within the date range
            end_time = parse(ad_squad["end_time"]) if ad_squad.get("end_time") else None
            if end_time and end_time < contextual_start_time:
                continue
            yield ad_squad

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        if record.get("start_time"):
            start_time = parse(record["start_time"])
        else:
            start_time = context["_sdc_start_time"]
        if record.get("end_time"):
            end_time = parse(record["end_time"])
        else:
            end_time = context.get("_sdc_end_time")

        return {
            "_sdc_timezone": context["_sdc_timezone"],
            "_sdc_start_time": start_time,
            "_sdc_end_time": end_time,
            "_sdc_adsquad_id": record["id"],
        }


class AdSquadStatsDailyStream(SnapchatStatsStream):
    """Define ad squad stats daily stream."""

    name = "ad_squad_stats_daily"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = AdSquadsStream
    path = "/adsquads/{_sdc_adsquad_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/stats.json"

    date_window_size = 30
    fields = ",".join(ALL_STAT_FIELDS)
    granularity = "DAY"


class AdSquadStatsHourlyStream(SnapchatStatsStream):
    """Define ad squad stats hourly stream."""

    name = "ad_squad_stats_hourly"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = AdSquadsStream
    path = "/adsquads/{_sdc_adsquad_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/stats.json"

    date_window_size = 7
    fields = ",".join(remove_invalid_hourly_metrics(ALL_STAT_FIELDS))
    granularity = "HOUR"


class AdsStream(SnapchatAdsStream):
    """Define ads stream."""

    name = "ads"
    json_key_array = "ads"
    json_key_record = "ad"
    parent_stream_type = AdSquadsStream
    path = "/adsquads/{_sdc_adsquad_id}/ads"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "ads.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        if record.get("created_at"):
            start_time = parse(record["created_at"])
        else:
            start_time = context.get("_sdc_start_time")
        return {
            "_sdc_timezone": context["_sdc_timezone"],
            "_sdc_start_time": start_time,
            "_sdc_ad_id": record["id"],
        }


class AdStatsDailyStream(SnapchatStatsStream):
    """Define ad stats daily stream."""

    name = "ad_stats_daily"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = AdsStream
    path = "/ads/{_sdc_ad_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/stats.json"

    date_window_size = 30
    fields = ",".join(ALL_STAT_FIELDS)
    granularity = "DAY"


class AdStatsHourlyStream(SnapchatStatsStream):
    """Define ad stats hourly stream."""

    name = "ad_stats_hourly"
    json_key_array = "timeseries_stats"
    json_key_record = "timeseries_stat"
    parent_stream_type = AdsStream
    path = "/ads/{_sdc_ad_id}/stats"
    primary_keys: t.ClassVar[list[str]] = ["id", "start_time"]
    replication_key = "end_time"
    schema_filepath = SCHEMAS_DIR / "shared/stats.json"

    date_window_size = 7
    fields = ",".join(remove_invalid_hourly_metrics(ALL_STAT_FIELDS))
    granularity = "HOUR"


class ProductCatalogsStream(SnapchatAdsStream):
    """Define product catalogs stream."""

    name = "product_catalogs"
    json_key_array = "catalogs"
    json_key_record = "catalog"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/catalogs"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"

    schema_filepath = SCHEMAS_DIR / "product_catalogs.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            "_sdc_catalog_id": record["id"],
        }


class ProductSetsStream(SnapchatAdsStream):
    """Define product sets stream."""

    name = "product_sets"
    json_key_array = "product_sets"
    json_key_record = "product_set"
    parent_stream_type = ProductCatalogsStream
    path = "/catalogs/{_sdc_catalog_id}/product_sets"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "product_sets.json"
