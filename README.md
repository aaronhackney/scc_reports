# scc_reports

This script downloads background search results from the Cisco Security Center (SCC) Firewall Management API.

## Usage

```bash
pip3 install -r requirements.txt
python3 download_files.py
```

## Environment Variables

.env file should contain the following variables:

API_KEY
API_URL
DOWNLOAD_DIR

## Example

    API_KEY = abc123
    API_URL = https://us.manage.security.cisco.com/api/platform/scc-gateway/request/swc/v2/download-status?per_tenant=true
    DOWNLOAD_DIR = "./files"