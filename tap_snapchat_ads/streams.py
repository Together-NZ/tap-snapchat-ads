"""Stream type classes for tap-snapchat-ads."""

from __future__ import annotations

import sys
import typing as t
from datetime import datetime, timedelta

import pendulum
from dateutil.parser import parse
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator

from tap_snapchat_ads.client import SnapchatAdsStream, SnapchatStatsStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

from typing import Any, Optional

from tap_snapchat_ads.fields import ALL_STAT_FIELDS

if t.TYPE_CHECKING:
    from requests import Response

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


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
    # replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "members.json"


class RolesStream(SnapchatAdsStream):
    """Define roles stream."""

    name = "roles"
    json_key_array = "roles"
    json_key_record = "role"
    parent_stream_type = OrganizationsStream
    path = "/organizations/{_sdc_org_id}/roles"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    # replication_key = "updated_at"
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

    def get_child_context(
        self, record: dict, context: dict | None
    ) -> dict | None:  # noqa: ARG002
        """Return a context dictionary for a child stream."""
        return {
            **context,
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

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Return a new paginator instance."""
        start_date = self.config.get("start_date")
        return self.TemporalPaginator(
            start_date, self.json_key_array, self.json_key_record
        )


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
            **context,
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

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            **context,
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

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            **context,
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
    parent_stream_type = AdAccountsStream
    path = "/adaccounts/{_sdc_adaccount_id}/ads"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "ads.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            **context,
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

    schema_filepath = SCHEMAS_DIR / "catalogs.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            **context,
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


class TargetingAgeGroupsStream(SnapchatAdsStream):
    """Define targeting age groups stream."""

    name = "targeting_age_groups"
    targeting_group = "demographics"
    targeting_type = "age_group"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingGendersStream(SnapchatAdsStream):
    """Define targeting genders stream."""

    name = "targeting_genders"
    targeting_group = "demographics"
    targeting_type = "gender"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingLanguagesStream(SnapchatAdsStream):
    """Define targeting languages stream."""

    name = "targeting_genders"
    targeting_group = "demographics"
    targeting_type = "languages"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingAdvancedDemographicsStream(SnapchatAdsStream):
    """Define targeting advanced demographics stream."""

    name = "targeting_advanced_demographics"
    targeting_group = "demographics"
    targeting_type = "advanced_demographics"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingConnectionTypesStream(SnapchatAdsStream):
    """Define targeting connection types stream."""

    name = "targeting_connection_types"
    targeting_group = "device"
    targeting_type = "connection_type"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingOSTypes(SnapchatAdsStream):
    """Define targeting os types stream."""

    name = "targeting_os_types"
    targeting_group = "device"
    targeting_type = "os_type"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingIOSVersionsStream(SnapchatAdsStream):
    """Define targeting ios versions stream."""

    name = "targeting_ios_versions"
    targeting_group = "device"
    targeting_type = "os_version"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/iOS/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingAndroidVersionsStream(SnapchatAdsStream):
    """Define targeting android versions stream."""

    name = "targeting_android_versions"
    targeting_group = "device"
    targeting_type = "os_version"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/ANDROID/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingCarrierStream(SnapchatAdsStream):
    """Define targeting carrier stream."""

    name = "targeting_carrier"
    targeting_group = "device"
    targeting_type = "carrier"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingDeviceMakesStream(SnapchatAdsStream):
    """Define targeting device makes stream."""

    name = "targeting_device_makes"
    targeting_group = "device"
    targeting_type = "marketing_name"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting.json"


class TargetingCountriesStream(SnapchatAdsStream):
    """Define targeting countries stream."""

    name = "targeting_countries"
    targeting_group = "geo"
    targeting_type = "country"

    json_key_array = "targeting_dimensions"
    json_key_record = f"{targeting_type}"
    path = f"/targeting/{targeting_group}/{targeting_type}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting_geo.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        """Return a context dictionary for a child stream."""
        return {
            "_sdc_country_id": record["country"]["code2"],
        }

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, Any]]:
        """Return a generator of records."""
        all_countries = super().get_records(context)
        selected_country_codes = self.config.get("geo_country_codes", [])
        if len(selected_country_codes) == 0:
            return

        for adaccount in all_countries:
            if adaccount["country"]["code2"] in selected_country_codes:
                yield adaccount


class TargetingRegionsStream(SnapchatAdsStream):
    """Define targeting regions stream."""

    name = "targeting_regions"
    targeting_group = "geo"
    targeting_type = "region"
    parent_stream_type = TargetingCountriesStream

    json_key_array = "targeting_dimensions"
    json_key_record = "region"
    path = "/targeting/geo/{_sdc_country_id}/region"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting_geo.json"


class TargetingMetrosStream(SnapchatAdsStream):
    """Define targeting metros stream."""

    name = "targeting_metros"
    targeting_group = "geo"
    targeting_type = "metro"
    parent_stream_type = TargetingCountriesStream

    json_key_array = "targeting_dimensions"
    json_key_record = "metro"
    path = "/targeting/geo/{_sdc_country_id}/metro"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting_geo.json"


class TargetingPostalCodesStream(SnapchatAdsStream):
    """Define targeting postalcodes stream."""

    name = "targeting_postal_codes"
    targeting_group = "geo"
    targeting_type = "postal_code"
    parent_stream_type = TargetingCountriesStream

    json_key_array = "targeting_dimensions"
    json_key_record = "postal_code"
    path = "/targeting/geo/{_sdc_country_id}/postal_code"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    schema_filepath = SCHEMAS_DIR / "shared/targeting_geo.json"
