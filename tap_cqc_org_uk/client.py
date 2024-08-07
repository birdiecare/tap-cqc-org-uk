"""REST client handling, including cqc-org-ukStream base class."""

import requests, datetime, backoff
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union, List, Iterable


from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class cqc_org_ukStream(RESTStream):
    """cqc-org-uk stream class."""

    url_base = "https://api.service.cqc.org.uk/public/v1"
    api_date_format = "%Y-%m-%dT%H:%M:%SZ"
    records_jsonpath = "$" 
    next_page_token_jsonpath = "$.nextPageUri" 


    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
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
         
        if response.status_code == 429: # 429 == too many requests
            raise RetriableAPIError(response.json()['message'])
        else:
            super().validate_response(response)

    def prepare_request(
            self, context: Optional[dict], next_page_token: Optional[Any]
        ) -> requests.PreparedRequest:

            request = super().prepare_request(context, next_page_token)

            # Set the User-Agent header to the partner code (a user agent header is required)
            request.headers["User-Agent"] = self.config["partner_code"]

            # Auth
            request.headers["Ocp-Apim-Subscription-Key"] = self.config["subscription_key"]

            # Replace the encoded colon in the URL with a regular colon (the API doesn't like the encoded version)
            # TODO: Apply urlencode python function in get_url_params method in streams instead. But, we will need to upgrade SDK version
            request.url = request.url.replace("%3A", ":")
            # print('Request URL: {url}'.format(url = request.url))
            self.logger.info(f"Request URL={request.url}")

            return request