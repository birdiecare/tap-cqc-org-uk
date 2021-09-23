"""Stream type classes for tap-cqc-org-uk."""

import requests, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_cqc_org_uk.client import cqc_org_ukStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ProviderIdsStream(cqc_org_ukStream):
    """Define stream of just the IDs for updated providers."""
    name = "ProviderIds"
    path = "/changes/provider"
    primary_keys = ["provider_id"]
    replication_key = "time_extracted"
    records_jsonpath = "$.changes[*]"  

    schema = th.PropertiesList(
        th.Property("provider_id", th.StringType),
        th.Property("time_extracted", th.DateTimeType)
    ).to_dict()

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:

        params = super().get_url_params(context, next_page_token)
        
        params["startTimestamp"] = self.get_starting_timestamp(context).strftime(self.api_date_format)
        params["endTimestamp"] = datetime.datetime.now().strftime(self.api_date_format)
        
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of results rows"""
        updated_providers = extract_jsonpath(self.records_jsonpath, input=response.json())

        for id in updated_providers:
            yield {"provider_id": id}
    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Create context for child 'Providers' stream"""
        return {
            "provider_id": record["provider_id"]
        }


class ProvidersStream(cqc_org_ukStream):
    """Define stream for all details of updated providers."""

    name = "Providers"
    path = "/providers/{provider_id}"
    parent_stream_type = ProviderIdsStream
    primary_keys = ["provider_id"]
    replication_key = "time_extracted"
    records_jsonpath = "$"
    schema_filepath = SCHEMAS_DIR / "Providers.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""    
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())