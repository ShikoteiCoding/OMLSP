import pytest
from src.requester import build_http_requester


@pytest.mark.trio
async def test_successful_request(vcr_cassette):
    """Test a successful GET request"""
    properties = {
        "connector": "http",
        "url": "https://httpbin.org/get",
        "method": "GET",
        "jq": "{url}",
    }
    http_requester_func = build_http_requester(properties)
    result = await http_requester_func()  # type: ignore
    assert result == [{"url": "https://httpbin.org/get"}]


@pytest.mark.trio
async def test_unsuccessful_request(vcr_cassette):
    """Test an unsuccessful GET request"""
    properties = {
        "connector": "http",
        "url": "https://httpbin.org/status/404",
        "method": "GET",
        "jq": "{url}",
    }
    http_requester_func = build_http_requester(properties)
    result = await http_requester_func()  # type: ignore
    assert result == []
