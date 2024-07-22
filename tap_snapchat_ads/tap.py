"""SnapchatAds tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_snapchat_ads import streams


class TapSnapchatAds(Tap):
    """SnapchatAds tap class."""

    name = "tap-snapchat-ads"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The client ID",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            description="The client secret",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "organization_ids",
            th.ArrayType(th.StringType),
            required=False,
            description="Organization IDs to replicate",
        ),
        th.Property(
            "adaccount_ids",
            th.ArrayType(th.StringType),
            required=False,
            description="AdAccount IDs to replicate",
        ),
        th.Property(
            "geo_country_codes",
            th.ArrayType(th.StringType),
            required=False,
            description="Geotargeting country codes to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        
    ).to_dict()

    def discover_streams(self) -> list[streams.SnapchatAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.OrganizationsStream(self),
            streams.FundingSourcesStream(self),
            streams.BillingCentersStream(self),
            streams.MembersStream(self),
            streams.RolesStream(self),
            streams.AdAccountsStream(self),
            streams.AdAccountsStatsDailyStream(self),
            streams.AdAccountsStatsHourlyStream(self),
            streams.AudienceSegementsStream(self),
            streams.PixelsStream(self),
            streams.PixelDomainStatsStream(self),
            streams.MediaStream(self),
            streams.CreativesStream(self),
            streams.CampaignsStream(self),
            streams.CampaignStatsDailyStream(self),
            streams.CampaignStatsHourlyStream(self),
            streams.AdSquadsStream(self),
            streams.AdSquadStatsDailyStream(self),
            streams.AdSquadStatsHourlyStream(self),
            streams.AdsStream(self),
            streams.AdStatsDailyStream(self),
            streams.AdStatsHourlyStream(self),
            streams.ProductCatalogsStream(self),
            streams.ProductSetsStream(self),
            # streams.TargetingAgeGroupsStream(self),
            # streams.TargetingGendersStream(self),
            # streams.TargetingAdvancedDemographicsStream(self),
            # streams.TargetingCarrierStream(self),
            # streams.TargetingDeviceMakesStream(self),
            # streams.TargetingCountriesStream(self),
            # streams.TargetingRegionsStream(self),
            # streams.TargetingMetrosStream(self),
            # streams.TargetingPostalCodesStream(self),
        ]


if __name__ == "__main__":
    TapSnapchatAds.cli()
