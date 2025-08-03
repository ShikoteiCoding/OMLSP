import pytest
from src.requester import build_http_requester

@pytest.mark.asyncio
async def test_successful_request(vcr_cassette):
    """Test a successful GET request"""
    properties = {
        "connector": "http",
        "url": "https://httpbin.org/get",
        "method": "GET",
        "json.jsonpath": "$.url",
    }
    http_requester_func = build_http_requester(properties)
    result = await http_requester_func()
    assert result == [{"url": "https://httpbin.org/get"}]

@pytest.mark.asyncio
async def test_unsuccessful_request(vcr_cassette):
    """Test an unsuccessful GET request"""
    properties = {
        "connector": "http",
        "url": "https://httpbin.org/status/404",
        "method": "GET",
        "json.jsonpath": "$.url",
    }
    http_requester_func = build_http_requester(properties)
    result = await http_requester_func()
    assert result == []