#!/usr/bin/env python3
"""
MARKET DATA FETCHER - DEBUG VERSION
"""

import os
import sys
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import json

print("=" * 70)
print("📊 MARKET DATA FETCHER - DEBUG MODE")
print("=" * 70)
print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# ===== CHECK DEPENDENCIES =====
print("🔍 Checking dependencies...")
try:
    import yfinance as yf
    print("✅ yfinance loaded")
except ImportError:
    print("❌ yfinance not installed!")
    print("Please add 'yfinance' to requirements.txt")
    sys.exit(1)

print("✅ All dependencies loaded")
print()

# ===== CONFIGURATION =====
WEB_APP_URL = os.environ.get('WEB_APP_URL')
if not WEB_APP_URL:
    print("❌ ERROR: WEB_APP_URL environment variable is not set!")
    print("\nTO FIX THIS:")
    print("1. Go to GitHub → Settings → Secrets and variables → Actions")
    print("2. Click 'New repository secret'")
    print("3. Name: WEB_APP_URL")
    print("4. Value: Your Google Apps Script Web App URL")
    print("5. Click 'Add secret'")
    sys.exit(1)

print(f"✅ Web App URL loaded: {WEB_APP_URL[:60]}...")
print()

PERIOD_DAYS = 200  # Reduced for testing
YAHOO_TICKERS = ['CSPX.L', 'EXUS.L', 'GLD', 'DX-Y.NYB']  # Reduced for testing
FRED_SERIES = {'DGS10': '10-Year Treasury'}  # Reduced for testing
V2TX_URL = "https://www.stoxx.com/document/Indices/Current/HistoricalData/h_v2tx.txt"

# ===== HELPER FUNCTIONS =====
def fetch_fred_data(series_id, days_back=30):
    """Fetch data from FRED"""
    try:
        fred_end = datetime.now().date()
        fred_start = fred_end - timedelta(days=days_back - 1)
        
        url = (
            f"https://fred.stlouisfed.org/graph/fredgraph.csv"
            f"?id={series_id}"
            f"&cosd={fred_start.strftime('%Y-%m-%d')}"
            f"&coed={fred_end.strftime('%Y-%m-%d')}"
        )
        
        print(f"    Fetching {url[:80]}...")
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            df = pd.read_csv(pd.io.common.StringIO(response.text))
            if len(df.columns) >= 2:
                df.columns = ['Date', series_id]
                df['Date'] = pd.to_datetime(df['Date'])
                print(f"    ✓ {series_id}: {len(df)} rows")
                return df.set_index('Date')[series_id]
            else:
                print(f"    ✗ {series_id}: Unexpected CSV format")
        else:
            print(f"    ✗ {series_id}: HTTP {response.status_code}")
                
    except Exception as e:
        print(f"    ✗ {series_id} error: {str(e)[:100]}")
    
    return pd.Series(dtype=float, name=series_id)

def send_to_google_sheets(csv_data, metadata):
    """Send data to Google Sheets with detailed debugging"""
    print("\n" + "="*70)
    print("📤 DEBUG: Preparing to send to Google Sheets")
    print("="*70)
    
    # Save CSV to debug file
    debug_filename = f"debug_csv_{datetime.now().strftime('%H%M%S')}.txt"
    with open(debug_filename, 'w', encoding='utf-8') as f:
        f.write(csv_data)
    
    print(f"💾 CSV saved locally: {debug_filename}")
    print(f"📏 CSV length: {len(csv_data)} characters")
    print(f"📄 CSV lines: {csv_data.count(chr(10))}")
    
    print("\n📋 CSV CONTENT (first 1000 chars):")
    print("-" * 50)
    print(csv_data[:1000])
    print("-" * 50)
    
    print("\n📋 CSV CONTENT (last 500 chars):")
    print("-" * 50)
    print(csv_data[-500:] if len(csv_data) > 500 else csv_data)
    print("-" * 50)
    
    # Check for problematic characters
    problem_chars = ['^', '&', '*', '(', ')', '[', ']', '{', '}', '|', '\\', ';', ':', '"', "'", '<', '>', '?', '/', '`', '~']
    found_problems = []
    for char in problem_chars:
        if char in csv_data:
            found_problems.append(char)
    
    if found_problems:
        print(f"⚠️  WARNING: CSV contains problematic characters: {found_problems}")
    
    # Prepare payload
    payload = {
        "csv_data": csv_data,
        "metadata": metadata
    }
    
    print(f"\n📦 Payload size: ~{len(json.dumps(payload))} characters")
    print(f"🌐 Sending to: {WEB_APP_URL[:80]}...")
    
    headers = {"Content-Type": "application/json"}
    
    try:
        print("🔄 Sending request...")
        response = requests.post(WEB_APP_URL, json=payload, headers=headers, timeout=60)
        
        print(f"✅ HTTP Status: {response.status_code}")
        print(f"📨 Response length: {len(response.text)} characters")
        
        if response.status_code == 200:
            try:
                result = response.json()
                print(f"📊 Response parsed successfully")
                return result
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON response: {e}")
                print(f"Response text (first 500 chars): {response.text[:500]}")
                return None
        else:
            print(f"❌ HTTP Error {response.status_code}")
            print(f"Response text (first 500 chars): {response.text[:500]}")
            return None
            
    except Exception as e:
        print(f"❌ Connection error: {type(e).__name__}: {str(e)}")
        return None

# ===== MAIN FUNCTION =====
def main():
    print("1. 📅 Calculating date range...")
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=PERIOD_DAYS - 1)
    
    print(f"   From: {start_date} to {end_date} ({PERIOD_DAYS} days)")
    print()
    
    # Dictionary to store all data
    all_data = {}
    data_summary = {}
    
    # ===== 2. FETCH YAHOO FINANCE DATA =====
    print("2. 📈 Fetching Yahoo Finance data...")
    for ticker in YAHOO_TICKERS:
        try:
            print(f"   {ticker}...", end=" ")
            
            # Use different method for indices
            if ticker.startswith('^'):
                stock = yf.Ticker(ticker)
                hist = stock.history(start=start_date, end=end_date + timedelta(days=1))
            else:
                hist = yf.download(ticker, start=start_date, end=end_date + timedelta(days=1), progress=False)
            
            if not hist.empty:
                if 'Close' in hist.columns:
                    # Clean column name
                    clean_name = ticker.replace('.L', '').replace('.NYB', '').replace('-', '_').replace('^', '')
                    all_data[clean_name] = hist['Close']
                    data_summary[clean_name] = f"{len(hist)} rows"
                    print(f"✓ {len(hist)} rows")
                else:
                    print(f"✗ No 'Close' column")
                    data_summary[ticker] = "No Close column"
            else:
                print(f"✗ No data")
                data_summary[ticker] = "No data"
                
        except Exception as e:
            print(f"✗ Error: {str(e)[:50]}")
            data_summary[ticker] = f"Error: {str(e)[:30]}"
    
    # ===== 3. FETCH V2TX DATA =====
    print("\n3. 🇪🇺 Fetching V2TX data...")
    try:
        response = requests.get(V2TX_URL, timeout=30)
        
        if response.status_code == 200:
            lines = response.text.splitlines()
            print(f"   Parsing {len(lines)} lines...")
            
            v2tx_data = []
            for line in lines[1:]:  # Skip header
                parts = line.split(';')
                if len(parts) == 3:
                    try:
                        date_str = parts[0].strip()
                        value = float(parts[2].strip())
                        date = pd.to_datetime(date_str, format='%d.%m.%Y')
                        v2tx_data.append((date, value))
                    except ValueError:
                        continue
            
            if v2tx_data:
                v2tx_df = pd.DataFrame(v2tx_data, columns=['Date', 'Close']).set_index('Date')
                
                # Filter to our date range
                v2tx_df = v2tx_df.loc[
                    (v2tx_df.index >= pd.to_datetime(start_date)) & 
                    (v2tx_df.index <= pd.to_datetime(end_date))
                ]
                
                if not v2tx_df.empty:
                    all_data['V2TX'] = v2tx_df['Close']
                    data_summary['V2TX'] = f"{len(v2tx_df)} rows"
                    print(f"   ✓ V2TX: {len(v2tx_df)} rows")
                else:
                    print(f"   ✗ No V2TX data in date range")
                    data_summary['V2TX'] = "No data in range"
            else:
                print("   ✗ Could not parse V2TX data")
                data_summary['V2TX'] = "Parse error"
        else:
            print(f"   ✗ HTTP Error {response.status_code}")
            data_summary['V2TX'] = f"HTTP {response.status_code}"
            
    except Exception as e:
        print(f"   ✗ Error: {e}")
        data_summary['V2TX'] = f"Error: {str(e)[:30]}"
    
    # ===== 4. FETCH FRED DATA =====
    print("\n4. 🏛️  Fetching FRED data...")
    for series_id in FRED_SERIES:
        try:
            print(f"   {series_id}...", end=" ")
            fred_series_data = fetch_fred_data(series_id, days_back=30)
            
            if not fred_series_data.empty:
                all_data[series_id] = fred_series_data
                data_summary[series_id] = f"{len(fred_series_data)} rows"
            else:
                print(f"✗ No data")
                data_summary[series_id] = "No data"
                
        except Exception as e:
            print(f"✗ Error: {e}")
            data_summary[series_id] = f"Error: {str(e)[:30]}"
    
    # ===== 5. COMBINE AND FORMAT DATA =====
    print("\n5. 🔄 Combining and formatting data...")
    
    if not all_data:
        print("❌ No data fetched from any source!")
        return False
    
    print(f"   Data sources: {len(all_data)}")
    
    # Create a complete date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    combined_df = pd.DataFrame(index=date_range)
    
    # Add each data series
    for name, series in all_data.items():
        combined_df[name] = series
    
    # Check if DataFrame has data
    if combined_df.empty:
        print("❌ Combined DataFrame is empty!")
        return False
    
    # Sort by date (newest first) and format
    combined_df = combined_df.sort_index(ascending=False)
    combined_df = combined_df.reset_index()
    combined_df.rename(columns={'index': 'Date'}, inplace=True)
    combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d')
    
    # ===== 6. CLEAN COLUMN NAMES =====
    print("\n6. 🔧 Cleaning column names...")
    
    # Remove problematic characters from column names
    original_columns = list(combined_df.columns)
    combined_df.columns = [col.replace('-', '_').replace('^', '').replace('.', '_') for col in combined_df.columns]
    
    print(f"   Original: {original_columns}")
    print(f"   Cleaned: {list(combined_df.columns)}")
    
    # ===== 7. FILL EMPTY VALUES =====
    print("\n7. 📊 Handling empty values...")
    
    # Replace NaN with empty string (not NaN)
    combined_df = combined_df.fillna('')
    
    # Verify no NaN remain
    nan_count = combined_df.isna().sum().sum()
    print(f"   ✓ NaN values after fill: {nan_count}")
    
    # Display summary
    print(f"\n   📈 Final DataFrame shape: {combined_df.shape[0]} rows × {combined_df.shape[1]} columns")
    
    print("\n   📋 Sample data (first 3 rows):")
    print(combined_df.head(3).to_string(index=False))
    
    # ===== 8. CREATE CSV =====
    print("\n8. 📄 Creating CSV...")
    
    # Create CSV with proper formatting
    csv_data = combined_df.to_csv(index=False)
    
    print(f"   ✓ CSV created: {len(csv_data)} characters")
    
    # ===== 9. SEND TO GOOGLE SHEETS =====
    print("\n9. 📤 Sending to Google Sheets...")
    
    metadata = {
        "date_generated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "period_days": PERIOD_DAYS,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "data_summary": data_summary
    }
    
    result = send_to_google_sheets(csv_data, metadata)
    
    if result:
        if result.get('success'):
            print(f"\n" + "="*70)
            print("🎉 SUCCESS! Data sent to Google Sheets")
            print("="*70)
            print(f"   Rows added: {result.get('rows_added', 'N/A')}")
            print(f"   Total historical rows: {result.get('historical_rows', 'N/A')}")
            print(f"   CSV received length: {result.get('csv_received_length', 'N/A')}")
            print(f"   CSV parsed rows: {result.get('csv_parsed_rows', 'N/A')}")
            
            # Save local backup
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"market_data_backup_{timestamp}.csv"
            combined_df.to_csv(backup_file, index=False)
            print(f"\n   💾 Local backup saved: {backup_file}")
            
            return True
        else:
            print(f"\n❌ Server returned error: {result.get('error', 'Unknown')}")
            return False
    else:
        print("\n❌ Failed to communicate with Google Sheets")
        return False

# ===== EXECUTION =====
if __name__ == "__main__":
    try:
        success = main()
        
        if success:
            print("\n" + "="*70)
            print("✅ MARKET DATA UPDATE COMPLETE")
            print("="*70)
            print(f"Next automatic update in 3 hours")
            print("Check Google Sheets for data")
            print("="*70)
            sys.exit(0)
        else:
            print("\n" + "="*70)
            print("⚠️  UPDATE FAILED - CHECK LOGS ABOVE")
            print("="*70)
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


