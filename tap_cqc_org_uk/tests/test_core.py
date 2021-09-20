"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_cqc_org_uk.tap import Tapcqc_org_uk

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "partner_code": "tap-cqc-org-uk-tests"
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        Tapcqc_org_uk,
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()

# TODO: Create additional tests as appropriate for your tap.
