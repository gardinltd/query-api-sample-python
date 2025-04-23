# Gardin API Query Example for Python

This repository contains a self-contained Python script demonstrating how to:
- Authenticate with the Gardin API using client credentials (OAuth2)
- Submit and monitor a query job
- Retrieve query results as a downloadable CSV file

## Prerequisites
- Python 3.7+
- `requests` package (install via `pip install requests`)
- Gardin API client credentials (CLIENT_ID and CLIENT_SECRET)

## Setup
1. Clone this repository.
2. Replace `<YOUR_CLIENT_ID>` and `<YOUR_CLIENT_SECRET>` with your credentials in `gardin_api_query_simple_example.py`.
3. Set your desired download path prefix via `CSV_DOWNLOAD_PATH_PREFIX`.

## Running the Script
```bash
pip install requests
python3 gardin_api_query_simple_example.py
```

## License
This library is licensed under the MIT-0 License. See the LICENSE file.
