"""REST client handling, including cqc-org-ukStream base class."""

from __future__ import annotations

import requests, datetime, backoff, sys
from typing import TYPE_CHECKING, Any, Callable

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import RetriableAPIError

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

if TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"

class cqc_org_ukStream(RESTStream):
    """cqc-org-uk stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root"""
        return "https://api.service.cqc.org.uk/public/v1"

    @property
    def api_date_format(self) -> str:
        """Return the date format used for API start and end date parameters."""
        return "%Y-%m-%dT%H:%M:%SZ"

    @property
    def records_jsonpath(self) -> str:
        """Return the JSON path to the records in the API response."""
        return "$"

    @property
    def next_page_token_jsonpath(self) -> str:
        """Return the JSON path expression for finding the next page token."""
        return "$.nextPageUri"

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Returns a new authenticator object."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Ocp-Apim-Subscription-Key",
            value=self.config.get("subscription_key", ""),
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Returns the http headers needed."""
        headers = {}
        headers["User-Agent"] = "Meltano Tap for CQC API: https://github.com/birdiecare/tap-cqc-org-uk"
        return headers


    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Returns a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.
        """
        params: dict = {}
        return params


    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        row["time_extracted"] = datetime.datetime.now().strftime(self.api_date_format)
        return row


    def request_decorator(self, func: Callable) -> Callable:
        """ Increases backoff factor and max tries from defaults (2 and 5 respectively) """
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
            ),
            max_tries=16,
            factor=10,
        )(func)
        return decorator


    def validate_response(self, response: requests.Response) -> None:
        """ Defines 429 (too many requests) as a retriable error """
        if response.status_code == 429: # 429 == too many requests
            raise RetriableAPIError(response.json()['message'])
        else:
            super().validate_response(response)