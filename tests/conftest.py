import pytest
import vcr

VCR_CONFIG = {
    "filter_headers": ["authorization"],
    "filter_query_parameters": ["timestamp", "nonce"],
    "record_mode": "once",
    "match_on": ["method", "uri"],
}


@pytest.fixture
def vcr_cassette(request):
    """VCR fixture"""
    cassette_path = f"tests/cassettes/{request.node.name}.yaml"
    with vcr.use_cassette(path=cassette_path, **VCR_CONFIG) as cassette:
        yield cassette
