"""Stream type classes for tap-cqc-org-uk."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_cqc_org_uk.client import cqc_org_ukStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class UpdatedProviderIdsStream(cqc_org_ukStream):
    """Define stream of just the IDs for updated providers."""
    name = "UpdatedProviderIds"
    path = "/changes/provider"
    primary_keys = ["provider_id"]
    replication_key = None
    records_jsonpath = "$.changes[*]"  # Or override `parse_response`.

    schema = th.PropertiesList(
        th.Property("provider_id", th.StringType)
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of results rows"""
        updated_providers = extract_jsonpath(self.records_jsonpath, input=response.json())

        for id in updated_providers:
            yield {"provider_id": id}
    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Create context for child 'UpdatedProviders' stream"""
        return {
            "provider_id": record["provider_id"]
        }


class UpdatedProvidersStream(cqc_org_ukStream):
    """Define stream for all details of updated providers."""

    name = "UpdatedProviders"
    path = "/providers/{provider_id}"
    parent_stream_type = UpdatedProviderIdsStream
    primary_keys = ["provider_id"]
    replication_key = None
    records_jsonpath = "$"

    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "updatedProviders.json"
    # schema = th.PropertiesList(
    #     th.Property("provider_id", th.StringType),
    #     th.Property("name", th.StringType),
    #     th.Property("organisationType", th.StringType)
    # ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""    
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())