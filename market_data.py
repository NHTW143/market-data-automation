#!/usr/bin/env python3
"""
MARKET DATA FETCHER - FULL VERSION
Fetches Yahoo Finance, V2TX, and FRED data
"""

import os
import sys
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

print("=" * 70)
print("📊 MARKET DATA FETCHER")
print("=" * 70)
print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Get Web App URL
WEB_APP_URL = os.environ.get('WEB_APP_URL')
if not WEB_APP_URL:
    print("❌ ERROR: WEB_APP_URL not set in GitHub Secrets!")
    sys.exit(1)

print(f"✅ Web App URL loaded")
print()

# ===== CONFIGURATION =====
PERIOD_DAYS = 200
YAHOO_TICKERS = ['CSPX.L', 'EXUS.L', 'GLD', 'DX-Y.NYB']  # Removed problematic ^ symbols
FRED_SERIES = {'T10YIE': '10-Year Breakeven', 'DGS10': '10-Year Treasury'}
V2TX_URL = "https://www.stoxx.com/document/Indices/Current/HistoricalData/h_v2tx.txt"

# ===== HELPER FUNCTIONS =====
def fetch_fred_data(series_id, days_back=150):
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
        
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            df = pd.read_csv(pd.io.common.StringIO(response.text))
            if len(df.columns) >= 2:
                df.columns = ['Date', series_id]
                df['Date'] = pd.to_datetime(df['Date'])
                return df.set_index('Date')[series_id]
    except Exception as e:
        print(f"    ✗ FRED {series_id}: {e}")
    return pd.Series(dtype=float, name=series_id)

def send_to_google_sheets(csv_data, metadata):
    """Send data to Google Sheets"""
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
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ HTTP Error {response.status_code}: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"❌ Connection error: {e}")
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
            data = yf.download(ticker, start=start_date, end=end_date + timedelta(days=1), progress=False)
            
            if not data.empty and 'Close' in data.columns:
                # Clean ticker name (remove .L, etc)
                clean_name = ticker.replace('.L', '').replace('.NYB', '')
                all_data[clean_name] = data['Close']
                data_summary[clean_name] = f"{len(data)} rows"
                print(f"✓ {len(data)} rows")
            else:
                print(f"✗ No data")
                data_summary[ticker] = "No data"
                
        except Exception as e:
            print(f"✗ Error: {str(e)[:50]}")
            data_summary[ticker] = f"Error"
    
    # ===== 3. FETCH V2TX DATA =====
    print("\n3. 🇪🇺 Fetching V2TX data...")
    try:
        response = requests.get(V2TX_URL, timeout=30)
        
        if response.status_code == 200:
            lines = response.text.splitlines()
            v2tx_data = []
            
            for line in lines[1:]:
                parts = line.split(';')
                if len(parts) == 3:
                    try:
                        date = pd.to_datetime(parts[0].strip(), format='%d.%m.%Y')
                        value = float(parts[2].strip())
                        v2tx_data.append((date, value))
                    except:
                        continue
            
            if v2tx_data:
                v2tx_df = pd.DataFrame(v2tx_data, columns=['Date', 'Close']).set_index('Date')
                v2tx_df = v2tx_df.loc[start_date:end_date]
                
                if not v2tx_df.empty:
                    all_data['V2TX'] = v2tx_df['Close']
                    data_summary['V2TX'] = f"{len(v2tx_df)} rows"
                    print(f"   ✓ V2TX: {len(v2tx_df)} rows")
                else:
                    print("   ✗ No data in range")
                    data_summary['V2TX'] = "No data"
            else:
                print("   ✗ Could not parse")
                data_summary['V2TX'] = "Parse error"
        else:
            print(f"   ✗ HTTP {response.status_code}")
            data_summary['V2TX'] = f"HTTP {response.status_code}"
            
    except Exception as e:
        print(f"   ✗ Error: {e}")
        data_summary['V2TX'] = f"Error"
    
    # ===== 4. FETCH FRED DATA =====
    print("\n4. 🏛️  Fetching FRED data...")
    for series_id in FRED_SERIES:
        try:
            print(f"   {series_id}...", end=" ")
            fred_data = fetch_fred_data(series_id, days_back=150)
            
            if not fred_data.empty:
                all_data[series_id] = fred_data
                data_summary[series_id] = f"{len(fred_data)} rows"
                print(f"✓ {len(fred_data)} rows")
            else:
                print(f"✗ No data")
                data_summary[series_id] = "No data"
                
        except Exception as e:
            print(f"✗ Error: {e}")
            data_summary[series_id] = f"Error"
    
    # ===== 5. COMBINE AND FORMAT DATA =====
    print("\n5. 🔄 Combining data...")
    
    if not all_data:
        print("❌ No data fetched!")
        return False
    
    print(f"   Sources: {len(all_data)} data series")
    
    # Create date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    combined_df = pd.DataFrame(index=date_range)
    
    # Add each data series
    for name, series in all_data.items():
        combined_df[name] = series
    
    # Sort by date (newest first) and format
    combined_df = combined_df.sort_index(ascending=False)
    combined_df = combined_df.reset_index()
    combined_df.rename(columns={'index': 'Date'}, inplace=True)
    combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d')
    
    # Fill NaN with empty string
    combined_df = combined_df.fillna('')
    
    print(f"   ✓ Final DataFrame: {combined_df.shape[0]} rows × {combined_df.shape[1]} columns")
    print(f"   ✓ Columns: {', '.join(combined_df.columns)}")
    
    # Show sample
    print("\n   📋 Sample (first 3 rows):")
    print(combined_df.head(3).to_string(index=False))
    print()
    
    # ===== 6. CREATE CSV =====
    csv_data = combined_df.to_csv(index=False)
    print(f"6. 📄 CSV created: {len(csv_data)} characters")
    
    # ===== 7. SEND TO GOOGLE SHEETS =====
    print("\n7. 📤 Sending to Google Sheets...")
    
    metadata = {
        "date_generated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "period_days": PERIOD_DAYS,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "data_summary": data_summary
    }
    
    result = send_to_google_sheets(csv_data, metadata)
    
    if result and result.get('success'):
        print(f"\n✅ SUCCESS! Data sent to Google Sheets")
        print(f"   Rows added: {result.get('rows_added', 'N/A')}")
        print(f"   Total historical rows: {result.get('historical_rows', 'N/A')}")
        
        # Save local backup
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        combined_df.to_csv(f"market_data_{timestamp}.csv", index=False)
        print(f"   💾 Local backup saved: market_data_{timestamp}.csv")
        
        return True
    else:
        print(f"\n❌ Failed: {result.get('error') if result else 'No response'}")
        return False

# ===== EXECUTION =====
if __name__ == "__main__":
    try:
        success = main()
        
        if success:
            print("\n" + "=" * 70)
            print("🎉 MARKET DATA UPDATE COMPLETE!")
            print("=" * 70)
            print(f"Next update: Every 3 hours")
            print(f"Check Google Sheets for data")
            print("=" * 70)
            sys.exit(0)
        else:
            print("\n" + "=" * 70)
            print("⚠️  UPDATE FAILED")
            print("=" * 70)
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
