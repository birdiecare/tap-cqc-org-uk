"""REST client handling, including cqc-org-ukStream base class."""

import requests, datetime, backoff
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
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

    def request_decorator(self, func: Callable) -> Callable:
        # Increase backoff factor and max tries from defaults (2 and 5 respectively)
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


    def validate_response(self, response):
         
        if response.json().get('statusCode') == 429: # 429 == too many requests
            raise RetriableAPIError(response.json()['message'])
        else:
            super().validate_response(response)