"""cqc_org_uk tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_cqc_org_uk import streams

class Tapcqc_org_uk(Tap):
    """cqc_org_uk tap class."""

    name = "tap-cqc-org-uk"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "subscription_key",
            th.StringType,
            required=True,
            secret=True,
            description="The primary key for the CQC API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest CQC update to sync",
        )
    ).to_dict()

    def discover_streams(self) -> list[streams.cqc_org_ukStream]:
        """Returns a list of discovered streams."""
        return [
            streams.CQC_ProvidersStream(self),
            streams.CQC_ProviderIdsStream(self),
            streams.CQC_LocationsStream(self),
            streams.CQC_LocationIdsStream(self)
        ]


if __name__ == "__main__":
    Tapcqc_org_uk.cli()
