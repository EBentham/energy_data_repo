from datetime import date
import re

from src.connectors.entsoe import EntsoeConnector


def extract_digits(s: str) -> str:
    return re.sub(r"\D", "", s)


def test_entsoe_url_and_params_construction():
    cfg = {
        "api_key": "MYENTSOE",
        "base_url": "https://web-api.tp.entsoe.eu/api",
        "primary_bidding_zone": "10YGB----------A",
        "queries": [
            {
                "name": "generation_per_type",
                "documentType": "A75",
                "params": {"processType": "A16"},
                "domain_params": {"in_Domain": "${primary_bidding_zone}"},
            }
        ],
    }

    conn = EntsoeConnector(cfg)

    query_cfg = cfg["queries"][0]
    url = conn.build_url_for_query(query_cfg)
    params = conn.build_params_for_query(query_cfg, query_date=date(2025, 3, 1))

    assert url == "https://web-api.tp.entsoe.eu/api"
    assert params.get("securityToken") == "MYENTSOE"
    assert params.get("documentType") == "A75"
    assert params.get("processType") == "A16"

    # periodStart / periodEnd present
    assert "periodStart" in params and "periodEnd" in params

    start_digits = extract_digits(params.get("periodStart", ""))
    end_digits = extract_digits(params.get("periodEnd", ""))
    # basic format check: only digits and at least YYYYMMDD
    assert re.match(r"^\d{8,}$", start_digits)
    assert re.match(r"^\d{8,}$", end_digits)

    # periodEnd should be later than periodStart (numeric compare on digit-only string)
    assert int(end_digits) > int(start_digits)

    # domain variable must be replaced
    assert params.get("in_Domain") == "10YGB----------A"


def test_period_format_for_various_dates():
    cfg = {
        "api_key": "MYENTSOE",
        "base_url": "https://web-api.tp.entsoe.eu/api",
        "primary_bidding_zone": "10YGB----------A",
        "queries": [
            {
                "name": "generation_per_type",
                "documentType": "A75",
                "params": {"processType": "A16"},
                "domain_params": {"in_Domain": "${primary_bidding_zone}"},
            }
        ],
    }

    conn = EntsoeConnector(cfg)
    query_cfg = cfg["queries"][0]

    for query_date, expected_prefix in [
        (date(2020, 1, 1), "20200101"),
        (date(2025, 12, 31), "20251231"),
    ]:
        params = conn.build_params_for_query(query_cfg, query_date=query_date)
        start_digits = extract_digits(params.get("periodStart", ""))
        assert start_digits.startswith(expected_prefix)


def test_additional_params_merge_and_domain_expansion():
    cfg = {
        "api_key": "MYENTSOE",
        "base_url": "https://web-api.tp.entsoe.eu/api",
        "primary_bidding_zone": "10YGB----------A",
        "queries": [
            {
                "name": "extended_query",
                "documentType": "DUMMY",
                "params": {"processType": "PX", "extraParam": "VALUE"},
                "domain_params": {"in_Domain": "${primary_bidding_zone}", "out_Domain": "STATIC"},
            }
        ],
    }

    conn = EntsoeConnector(cfg)
    query_cfg = cfg["queries"][0]
    params = conn.build_params_for_query(query_cfg, query_date=date(2025, 6, 15))

    assert params.get("processType") == "PX"
    assert params.get("extraParam") == "VALUE"
    assert params.get("in_Domain") == cfg["primary_bidding_zone"]
    assert params.get("out_Domain") == "STATIC"


# Simple runner to allow executing this file directly for debugging in an IDE.
if __name__ == "__main__":
    tests = [
        test_entsoe_url_and_params_construction,
        test_period_format_for_various_dates,
        test_additional_params_merge_and_domain_expansion,
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
