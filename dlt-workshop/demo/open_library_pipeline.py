"""dlt pipeline to ingest data from the Open Library REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from Open Library REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "/api/books",
                    "method": "GET",
                    "params": {
                        "bibkeys": "ISBN:0451526538,ISBN:0451526546",
                        "format": "json",
                        "jscmd": "details",
                    },
                    "data_selector": "[*]",  # Extract all values from the root object
                    "paginator": {
                        "type": "single_page",  # No pagination for this endpoint
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
