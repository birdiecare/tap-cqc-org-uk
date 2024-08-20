"""Tests standard tap features using the built-in SDK tests library."""

import datetime, os
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.DEBUG)

from singer_sdk.testing import get_tap_test_class

from tap_cqc_org_uk.tap import Tapcqc_org_uk

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "subscription_key": os.getenv("TEST_CQC_SUBSCRIPTION_KEY"),
}


# Run standard built-in tap tests from the SDK:
TestTapcqc_org_uk = get_tap_test_class(
    tap_class=Tapcqc_org_uk,
    config=SAMPLE_CONFIG,
)


def test_subscription_key_in_headers():
    tap = Tapcqc_org_uk(config={
        "start_date": "2024-01-01",
        "subscription_key": "test_key",
    })
    stream = tap.streams["CQC_Providers"]
    prepared_request = stream.prepare_request(context={}, next_page_token=None)

    # Check that the subscription key is in the headers
    assert "Ocp-Apim-Subscription-Key" in prepared_request.headers
    assert prepared_request.headers["Ocp-Apim-Subscription-Key"] == "test_key"