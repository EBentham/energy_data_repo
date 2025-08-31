import pytest
from datetime import date
from urllib.parse import urlencode

from src.connectors.elexon import ElexonConnector
from src.connectors.entsoe import EntsoeConnector


class FakeSession:
    def __init__(self):
        # emulate requests.Session.params attribute
        self.params = None
        self.last_get = None

    def get(self, url, params=None):
        # emulate how requests would merge session.params and params passed to get()
        merged = {}
        if self.params:
            merged.update(self.params)
        if params:
            merged.update(params)
        self.last_get = {"url": url, "params": merged}

        class Resp:
            status_code = 200

            def __init__(self, url):
                self.text = "<xml></xml>"
                self.url = url

        resp_url = url
        if merged:
            resp_url = url + "?" + urlencode(merged)
        return Resp(resp_url)


def test_elexon_simple_report_url_and_params():
    cfg = {
        "api_key": "MYELEXONKEY",
        "base_url": "https://api.bmreports.com/BMRS",
        "reports": [
            {"name": "generation_registered_capacity", "code": "B1430", "version": "v1", "params": {}}
        ],
    }

    conn = ElexonConnector(cfg)
    # inject fake session
    conn._client.session = FakeSession()

    # run extract for a single day (the connector will iterate reports and call client._make_request)
    out = conn.extract(date(2025, 1, 1), date(2025, 1, 1))

    # verify the fake session recorded the GET call
    last = conn._client.session.last_get
    assert last is not None, "No GET recorded by fake session for Elexon"

    expected_url = "https://api.bmreports.com/BMRS/B1430/v1"
    assert last["url"] == expected_url
    assert last["params"].get("APIKey") == "MYELEXONKEY"
    assert last["params"].get("ServiceType") == "xml"


def test_elexon_settlement_date_period_params():
    cfg = {
        "api_key": "MYELEXONKEY",
        "base_url": "https://api.bmreports.com/BMRS",
        "reports": [
            {"name": "bid_offer_level_data", "code": "BOALF", "version": "v1", "date_param_type": "settlement_date_period", "params": {}}
        ],
    }

    conn = ElexonConnector(cfg)
    conn._client.session = FakeSession()

    # extract will call for each period (1..48) for the single date
    out = conn.extract(date(2025, 2, 2), date(2025, 2, 2))

    last = conn._client.session.last_get
    assert last is not None
    assert last["params"].get("SettlementDate") == "2025-02-02"
    # Period should be present and be a string integer between '1' and '48'
    assert "Period" in last["params"]
    assert last["params"]["Period"].isdigit()


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
    # replace client's session with fake one; Entsoe client sets session.params during init
    conn._client.session = FakeSession()
    # ensure the security token is set on session.params (the real client would set this)
    conn._client.session.params = {"securityToken": "MYENTSOE"}

    out = conn.extract(date(2025, 3, 1), date(2025, 3, 1))

    last = conn._client.session.last_get
    assert last is not None
    assert last["url"] == "https://web-api.tp.entsoe.eu/api"
    assert last["params"].get("securityToken") == "MYENTSOE"
    assert last["params"].get("documentType") == "A75"
    assert last["params"].get("processType") == "A16"
    assert "periodStart" in last["params"] and "periodEnd" in last["params"]

