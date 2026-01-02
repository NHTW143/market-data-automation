#!/usr/bin/env python3
"""
MARKET DATA FETCHER - ULTRA SIMPLE TEST
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime

print("=" * 70)
print("🚀 MARKET DATA TEST STARTING")
print("=" * 70)
print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Get Web App URL
WEB_APP_URL = os.environ.get('WEB_APP_URL')
if not WEB_APP_URL:
    print("❌ ERROR: WEB_APP_URL environment variable is empty!")
    print()
    print("TO FIX THIS:")
    print("1. Go to your GitHub repository")
    print("2. Click 'Settings' → 'Secrets and variables' → 'Actions'")
    print("3. Click 'New repository secret'")
    print("4. Name: WEB_APP_URL")
    print("5. Value: Your Google Apps Script Web App URL")
    print("6. Click 'Add secret'")
    print("7. Run this workflow again")
    sys.exit(1)

print(f"✅ Web App URL found (first 60 chars):")
print(f"   {WEB_APP_URL[:60]}...")
print()

# Create SIMPLE test data
print("🧪 Creating test data...")
test_df = pd.DataFrame({
    'Date': ['2026-01-02', '2026-01-01'],
    'CSPX': [733.52, 732.10],
    'GLD': [397.94, 396.31]
})

print(f"Test DataFrame:")
print(test_df)
print()
print(f"Shape: {test_df.shape}")
print(f"Columns: {list(test_df.columns)}")
print()

# Convert to CSV
csv_data = test_df.to_csv(index=False)
print(f"📄 CSV data ({len(csv_data)} characters):")
print(csv_data)
print()

# Prepare payload
payload = {
    "csv_data": csv_data,
    "metadata": {
        "test": "github_actions",
        "timestamp": datetime.now().isoformat(),
        "python_version": sys.version.split()[0]
    }
}

print("📤 Sending to Google Sheets...")
print(f"URL: {WEB_APP_URL[:80]}...")
print()

try:
    response = requests.post(
        WEB_APP_URL,
        json=payload,
        headers={'Content-Type': 'application/json'},
        timeout=30
    )
    
    print(f"✅ HTTP Status: {response.status_code}")
    print(f"Response: {response.text}")
    print()
    
    if response.status_code == 200:
        result = response.json()
        if result.get('success'):
            print("🎉 SUCCESS! Data accepted by Google Sheets")
            print(f"Rows added: {result.get('rows_added', 'N/A')}")
            print(f"Total historical rows: {result.get('historical_rows', 'N/A')}")
            sys.exit(0)
        else:
            print("❌ Google Apps Script returned error:")
            print(f"Error: {result.get('error', 'Unknown')}")
            sys.exit(1)
    else:
        print(f"❌ HTTP Error {response.status_code}")
        sys.exit(1)
        
except Exception as e:
    print(f"❌ Request failed:")
    print(f"Error type: {type(e).__name__}")
    print(f"Error message: {str(e)}")
    sys.exit(1)
