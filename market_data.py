#!/usr/bin/env python3
"""
MARKET DATA FETCHER - SIMPLE VERSION
Fetches data and sends to Google Sheets
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta

print("=" * 70)
print("MARKET DATA FETCHER STARTING")
print("=" * 70)
print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Get Web App URL
WEB_APP_URL = os.environ.get('WEB_APP_URL')
if not WEB_APP_URL:
    print("❌ ERROR: WEB_APP_URL not found in environment variables!")
    print("Check GitHub Secrets → Actions → WEB_APP_URL")
    sys.exit(1)

print(f"✅ Web App URL loaded (first 50 chars): {WEB_APP_URL[:50]}...")
print()

# ===== TEST 1: SIMPLE CSV =====
print("🧪 TEST 1: Creating simple test CSV...")

# Create perfect test data
test_data = {
    'Date': ['2026-01-02', '2026-01-01', '2025-12-31'],
    'CSPX': [733.52, 732.10, 738.53],
    'GLD': [397.94, 396.31, 395.80],
    'Treasury': [4.12, 4.10, 4.08]
}

df = pd.DataFrame(test_data)
csv_data = df.to_csv(index=False)

print(f"Test DataFrame shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print()
print("CSV content:")
print(csv_data)
print()

# Send test data
print("📤 Sending test data to Google Sheets...")
metadata = {
    "test": "simple_data",
    "timestamp": datetime.now().isoformat(),
    "columns": len(df.columns)
}

payload = {
    "csv_data": csv_data,
    "metadata": metadata
}

try:
    response = requests.post(
        WEB_APP_URL,
        json=payload,
        headers={'Content-Type': 'application/json'},
        timeout=30
    )
    
    print(f"✅ Response status: {response.status_code}")
    print(f"Response: {response.text}")
    
    if response.status_code == 200:
        result = response.json()
        if result.get('success'):
            print(f"\n🎉 SUCCESS! Test data accepted.")
            print(f"Rows added: {result.get('rows_added', 'N/A')}")
            sys.exit(0)  # Success!
        else:
            print(f"\n❌ Server error: {result.get('error', 'Unknown')}")
            sys.exit(1)
    else:
        print(f"\n❌ HTTP error: {response.status_code}")
        sys.exit(1)
        
except Exception as e:
    print(f"\n❌ Connection error: {e}")
    sys.exit(1)