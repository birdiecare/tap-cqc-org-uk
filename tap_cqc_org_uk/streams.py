"""Stream type classes for tap-cqc-org-uk."""

import requests, datetime
from urllib.parse import urlparse
from urllib.parse import parse_qs
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_cqc_org_uk.client import cqc_org_ukStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

# PROVIDER STREAMS
class CQC_ProviderIdsStream(cqc_org_ukStream):
    """Define stream for the IDs of updated providers."""
    name = "CQC_ProviderIds"
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
        
        if next_page_token:
            next_page_token_query = parse_qs(urlparse(next_page_token).query)
            params["page"] = next_page_token_query["page"][0]
        
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



class CQC_ProvidersStream(cqc_org_ukStream):
    """Define stream for all details of updated providers."""

    name = "CQC_Providers"
    path = "/providers/{provider_id}"
    parent_stream_type = CQC_ProviderIdsStream
    primary_keys = ["provider_id"]
    replication_key = "time_extracted"
    records_jsonpath = "$"
    schema_filepath = SCHEMAS_DIR / "CQC_Providers.json"

    def _request_with_backoff(self, prepared_request, context: Optional[dict]) -> requests.Response:

        response = self.requests_session.send(prepared_request)

        # Ignore provider ids with records that cannot be found
        if response.status_code == 404:
            return response

        return super()._request_with_backoff(prepared_request, context)


# LOCATION STREAMS
class CQC_LocationIdsStream(cqc_org_ukStream):
    """Define stream for the IDs of updated locations."""
    name = "CQC_LocationIds"
    path = "/changes/location"
    primary_keys = ["location_id"]
    replication_key = "time_extracted"
    records_jsonpath = "$.changes[*]"  

    schema = th.PropertiesList(
        th.Property("location_id", th.StringType),
        th.Property("time_extracted", th.DateTimeType)
    ).to_dict()


    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:

        params = super().get_url_params(context, next_page_token)
        
        params["startTimestamp"] = self.get_starting_timestamp(context).strftime(self.api_date_format)
        params["endTimestamp"] = datetime.datetime.now().strftime(self.api_date_format)
        
        if next_page_token:
            next_page_token_query = parse_qs(urlparse(next_page_token).query)
            params["page"] = next_page_token_query["page"][0]
        
        return params


    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of results rows"""
        updated_locations = extract_jsonpath(self.records_jsonpath, input=response.json())

        for id in updated_locations:
            yield {"location_id": id}
    

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Create context for child 'Locations' stream"""
        return {
            "location_id": record["location_id"]
        }



class CQC_LocationsStream(cqc_org_ukStream):
    """Define stream for all details of updated locations."""

    name = "CQC_Locations"
    path = "/locations/{location_id}"
    parent_stream_type = CQC_LocationIdsStream
    primary_keys = ["locationId"]
    replication_key = "time_extracted"
    records_jsonpath = "$"
    schema_filepath = SCHEMAS_DIR / "CQC_Locations.json"

    def _request_with_backoff(self, prepared_request, context: Optional[dict]) -> requests.Response:

        response = self.requests_session.send(prepared_request)

        # Ignore location ids with records that cannot be found
        if response.status_code == 404:
            return response

        return super()._request_with_backoff(prepared_request, context)
