from datetime import date

from src.connectors.elexon import ElexonConnector


def test_elexon_simple_report_url_and_params():
    cfg = {
        "api_key": "MYELEXONKEY",
        "base_url": "https://api.bmreports.com/BMRS",
        "reports": [
            {"name": "generation_registered_capacity", "code": "B1430", "version": "v1", "params": {}}
        ],
    }

    conn = ElexonConnector(cfg)

    report_cfg = cfg['reports'][0]
    url = conn.build_url_for_report(report_cfg)
    params = conn.build_params_for_report(report_cfg)

    expected_url = "https://api.bmreports.com/BMRS/B1430/v1"
    assert url == expected_url
    assert params.get("APIKey") == "MYELEXONKEY"
    assert params.get("ServiceType") == "xml"


def test_elexon_settlement_date_period_params():
    cfg = {
        "api_key": "MYELEXONKEY",
        "base_url": "https://api.bmreports.com/BMRS",
        "reports": [
            {"name": "bid_offer_level_data", "code": "BOALF", "version": "v1", "date_param_type": "settlement_date_period", "params": {}}
        ],
    }

    conn = ElexonConnector(cfg)
    report_cfg = cfg['reports'][0]

    # Build params for a specific date and period
    params = conn.build_params_for_report(report_cfg, current_date=date(2025, 2, 2), period=5)

    assert params.get("SettlementDate") == "2025-02-02"
    assert params.get("Period") == "5"
    assert params.get("APIKey") == "MYELEXONKEY"


def test_b1620_agpt_url_and_params():
    """Test B1620 (AGPT) URL and from/to date parameters."""
    cfg = {
        "api_key": "MYELEXONKEY",
        "base_url": "https://api.bmreports.com/BMRS",
        "reports": [
            {"name": "actual_aggregated_generation_per_type", "code": "B1620", "version": "v1", "date_param_type": "from_to_date", "params": {}}
        ],
    }
    conn = ElexonConnector(cfg)
    report_cfg = cfg['reports'][0]

    url = conn.build_url_for_report(report_cfg)
    params = conn.build_params_for_report(report_cfg, current_date=date(2025, 1, 1))

    assert url == "https://api.bmreports.com/BMRS/B1620/v1"
    assert params.get("FromDate") == "2025-01-01"
    assert params.get("ToDate") == "2025-01-01"
    assert params.get("APIKey") == "MYELEXONKEY"


def test_b1610_total_load_url_and_settlement_params():
    """Test B1610 (Actual Total Load) URL and settlement date/period params."""
    cfg = {
        "api_key": "MYELEXONKEY",
        "base_url": "https://api.bmreports.com/BMRS",
        "reports": [
            {"name": "actual_total_load", "code": "B1610", "version": "v1", "date_param_type": "settlement_date_period", "params": {}}
        ],
    }
    conn = ElexonConnector(cfg)
    report_cfg = cfg['reports'][0]

    url = conn.build_url_for_report(report_cfg)
    params = conn.build_params_for_report(report_cfg, current_date=date(2025, 7, 1), period=36)

    assert url == "https://api.bmreports.com/BMRS/B1610/v1"
    assert params.get("SettlementDate") == "2025-07-01"
    assert params.get("Period") == "36"
    assert params.get("APIKey") == "MYELEXONKEY"


# Simple runner to allow executing this file directly for debugging in an IDE.
if __name__ == "__main__":
    tests = [
        test_b1620_agpt_url_and_params,
        test_b1610_total_load_url_and_settlement_params,
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
