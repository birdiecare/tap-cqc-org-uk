"""REST client handling, including cqc-org-ukStream base class."""

import requests, datetime, backoff
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class cqc_org_ukStream(RESTStream):
    """cqc-org-uk stream class."""

    url_base = "https://api.cqc.org.uk/public/v1"
    api_date_format = "%Y-%m-%dT%H:%M:%SZ"
    records_jsonpath = "$" 
    next_page_token_jsonpath = "$.nextPageUri" 


    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["partnerCode"] = self.config["partner_code"]
        return params


    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row["time_extracted"] = datetime.datetime.now().strftime(self.api_date_format)
        return row


    # Handle "Error 429 Rate limit exceeded" 
    # Many thanks to Pablo Seibelt (https://meltano.slack.com/team/U01VA6FNM55) see this slack thread...
    # https://meltano.slack.com/archives/C01PKLU5D1R/p1628695715006300?thread_ts=1628695448.005500&cid=C01PKLU5D1R
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=16,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code != 429,
        factor=10,
    )
    def _request_with_backoff(
        self, prepared_request, context: Optional[dict]
    ) -> requests.Response:
        response = self.requests_session.send(prepared_request)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = cast(str, prepared_request.path_url)
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        if response.status_code in [401, 403]:
            self.logger.info("Failed request for {}".format(prepared_request.url))
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        if response.status_code == 429:
            self.logger.info("Throttled request for {}".format(prepared_request.url))
            raise requests.exceptions.RequestException(
                request=prepared_request,
                response=response
            )
        elif response.status_code >= 400:
            raise RuntimeError(
                f"Error making request to API: {prepared_request.url} "
                f"[{response.status_code} - {str(response.content)}]".replace(
                    "\\n", "\n"
                )
            )

        return response
