from datetime import date
from urllib.parse import urlparse, parse_qs
import requests

from src.connectors.elexon.parameter_builder import ElexonParameterBuilder

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"


def test_params_b1610():
    report_cfg = {
        "name": "actual_total_load",
        "code": "B1610",
        "version": "v1",
        "date_param_type": "settlement_date_period",
        "params": {"settlementPeriodFrom": 1, "settlementPeriodTo": 36},
    }

    builder = ElexonParameterBuilder()
    params = builder.build_params_for_report(report_cfg, start_date=date(2023, 7, 18), end_date=date(2023, 7, 18))

    assert params.get("from") == "2023-07-18"
    assert params.get("to") == "2023-07-18"
    assert params.get("settlementPeriodFrom") == 1 or params.get("settlementPeriodFrom") == "1"
    assert params.get("settlementPeriodTo") == 36 or params.get("settlementPeriodTo") == "36"
    assert params.get("format") == "json"


def test_url_b1610():
    report_cfg = {
        "name": "actual_total_load",
        "code": "B1610",
        "version": "v1",
        "date_param_type": "settlement_date_period",
        "params": {"settlementPeriodFrom": 1, "settlementPeriodTo": 36},
    }

    builder = ElexonParameterBuilder()
    url = builder.build_full_request_url(report_cfg, BASE_URL, start_date=date(2023, 7, 18), end_date=date(2023, 7, 18))

    parsed = urlparse(url)
    assert parsed.scheme in ("http", "https")
    assert parsed.netloc == "data.elexon.co.uk"
    assert parsed.path.endswith("/demand/actual/total")

    qs = parse_qs(parsed.query)
    # parse_qs returns list values
    assert qs.get("from")[0] == "2023-07-18"
    assert qs.get("to")[0] == "2023-07-18"
    assert qs.get("settlementPeriodFrom")[0] in ("1", "1")
    assert qs.get("settlementPeriodTo")[0] in ("36", "36")
    assert qs.get("format")[0] == "json"


def test_ping_b1610_returns_json():
    report_cfg = {
        "name": "actual_total_load",
        "code": "B1610",
        "version": "v1",
        "date_param_type": "settlement_date_period",
        "params": {"settlementPeriodFrom": 1, "settlementPeriodTo": 36},
    }

    builder = ElexonParameterBuilder()
    url = builder.build_full_request_url(report_cfg, BASE_URL, start_date=date(2023, 7, 18), end_date=date(2023, 7, 18))

    print("Requesting:", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    # Try parsing JSON
    try:
        data = resp.json()
    except Exception as e:
        raise AssertionError(f"Response from Elexon is not valid JSON: {e}")

    assert data is not None
    # basic shape check: either dict or list
    assert isinstance(data, (dict, list))


def test_params_b1620():
    report_cfg = {
        "name": "actual_aggregated_generation_per_type",
        "code": "B1620",
        "version": "v1",
        "date_param_type": "from_to_date",
        "params": {"settlementPeriodFrom": 1, "settlementPeriodTo": 36},
    }

    builder = ElexonParameterBuilder()
    params = builder.build_params_for_report(report_cfg, start_date=date(2023, 7, 20), end_date=date(2023, 7, 20))

    assert params.get("from") == "2023-07-20"
    assert params.get("to") == "2023-07-20"
    assert params.get("settlementPeriodFrom") == 1 or params.get("settlementPeriodFrom") == "1"
    assert params.get("settlementPeriodTo") == 36 or params.get("settlementPeriodTo") == "36"
    assert params.get("format") == "json"


def test_url_b1620_and_ping():
    report_cfg = {
        "name": "actual_aggregated_generation_per_type",
        "code": "B1620",
        "version": "v1",
        "date_param_type": "from_to_date",
        "params": {"settlementPeriodFrom": 1, "settlementPeriodTo": 36},
    }

    builder = ElexonParameterBuilder()
    url = builder.build_full_request_url(report_cfg, BASE_URL, start_date=date(2023, 7, 20), end_date=date(2023, 7, 20))

    parsed = urlparse(url)
    assert parsed.path.endswith("/generation/actual/per-type")

    qs = parse_qs(parsed.query)
    assert qs.get("from")[0] == "2023-07-20"
    assert qs.get("to")[0] == "2023-07-20"
    assert qs.get("settlementPeriodFrom")[0] in ("1", "1")
    assert qs.get("settlementPeriodTo")[0] in ("36", "36")
    assert qs.get("format")[0] == "json"

    print("Requesting:", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    try:
        data = resp.json()
    except Exception as e:
        raise AssertionError(f"Response from Elexon is not valid JSON: {e}")
    assert isinstance(data, (dict, list))


if __name__ == '__main__':
    tests = [
        test_params_b1610,
        test_url_b1610,
        test_ping_b1610_returns_json,
        test_params_b1620,
        test_url_b1620_and_ping,
    ]

    failures = []
    for t in tests:
        try:
            t()
            print(f"{t.__name__}: PASS")
        except AssertionError as e:
            failures.append((t.__name__, str(e)))
            print(f"{t.__name__}: FAIL - {e}")
        except Exception as e:
            failures.append((t.__name__, str(e)))
            print(f"{t.__name__}: ERROR - {e}")

    if failures:
        print(f"\n{len(failures)} test(s) failed")
        raise SystemExit(1)
    else:
        print("\nAll tests passed")

