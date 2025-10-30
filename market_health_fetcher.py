"""
Market Health Fetcher - GitHub Auto-Upload Version
Perfect for Railway + PythonAnywhere combo!
"""

import json
import time
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime
import os
import base64
import pytz

IST = pytz.timezone("Asia/Kolkata")
EXCEL_FILE = "market_health_data.xlsx"
UPDATE_INTERVAL = 120  # 2 minutes

# GitHub Config - Set these in Railway Environment Variables
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', 'your-token-here')
GITHUB_REPO = os.environ.get('GITHUB_REPO', 'your-username/market-health-data')
GITHUB_BRANCH = os.environ.get('GITHUB_BRANCH', 'main')

# Chartink URLs
CI_HOME = "https://chartink.com"
CI_SCREENER = f"{CI_HOME}/screener"
CI_WIDGET_PROCESS = f"{CI_HOME}/widget/process"

# YOUR QUERIES
QUERIES = {
    "Top Gainers": {
        "query": "select latest Close - 1 day ago Close / 1 day ago Close * 100 as 'DAILY', latest Close - 1 week ago Close / 1 week ago Close * 100 as 'WEEKLY', latest Close - 1 month ago Close / 1 month ago Close * 100 as 'MONTHLY' WHERE( {cash} ( latest close > 1 day ago close and market cap > 1000 ) ) GROUP BY symbol ORDER BY 1 desc",
        "icon": "🚀"
    },
    "Top Losers": {
        "query": "select ( ( latest Close - 1 day ago Close ) / 1 day ago Close ) * 100 as 'DAILY', ( ( latest Close - 1 week ago Close ) / 1 week ago Close ) * 100 as 'WEEKLY', ( ( latest Close - 1 month ago Close ) / 1 month ago Close ) * 100 as 'MONTHLY' WHERE( {cash} ( latest close < 1 day ago close and market cap > 1000 ) ) GROUP BY symbol ORDER BY 1 asc",
        "icon": "📉"
    },
    "1 Month Performance": {
        "query": "select ( ( latest Close - 30 days ago Close ) / 30 days ago Close ) * 100 as '% change' WHERE( {cash} ( latest close > 20 and market cap > 500 ) ) GROUP BY symbol ORDER BY 1 desc",
        "icon": "📈"
    },
    "distance from Dma50": {
        "query": "select latest Close - latest Sma( latest Close , 50 ) / latest Sma( latest Close , 50 ) * 100 as 'Distance from SMA50' WHERE {45603} 1 = 1 GROUP BY symbol ORDER BY 1 desc",
        "icon": "📈"
    },
    "Retail Shareholding Increase": {
        "query": "select 3 quarter ago {custom_indicator_68140_start}\"total percentage - (  total foreign promoter and group percentage +  indian promoter and group percentage +  {custom_indicator_52652_start}\"foreign institution other percentage +  foreign direct investments institutions percentage +  foreign institutional investors percentage +  foreign bank percentage +  foreign collaborators percentage +  foreign venture capital investors percentage +  foreign non institution other percentage\"{custom_indicator_52652_end} +  {custom_indicator_52657_start}\"mutual funds or uti percentage +  insurance companies percentage +  others institutions percentage +  clearing members percentage +  corporate bodies percentage +  govt central or state percentage +  trusts institutes percentage +  venture capital funds percentage +  nsdl intransit percentage +  financial institutions or banks percentage\"{custom_indicator_52657_end} +  others non promoter non institution percentage )\"{custom_indicator_68140_end} as '% 3 Qtrs ago', 2 quarter ago {custom_indicator_68140_start}\"total percentage - (  total foreign promoter and group percentage +  indian promoter and group percentage +  {custom_indicator_52652_start}\"foreign institution other percentage +  foreign direct investments institutions percentage +  foreign institutional investors percentage +  foreign bank percentage +  foreign collaborators percentage +  foreign venture capital investors percentage +  foreign non institution other percentage\"{custom_indicator_52652_end} +  {custom_indicator_52657_start}\"mutual funds or uti percentage +  insurance companies percentage +  others institutions percentage +  clearing members percentage +  corporate bodies percentage +  govt central or state percentage +  trusts institutes percentage +  venture capital funds percentage +  nsdl intransit percentage +  financial institutions or banks percentage\"{custom_indicator_52657_end} +  others non promoter non institution percentage )\"{custom_indicator_68140_end} as '% 2 Qtr ago', 1 quarter ago {custom_indicator_68140_start}\"total percentage - (  total foreign promoter and group percentage +  indian promoter and group percentage +  {custom_indicator_52652_start}\"foreign institution other percentage +  foreign direct investments institutions percentage +  foreign institutional investors percentage +  foreign bank percentage +  foreign collaborators percentage +  foreign venture capital investors percentage +  foreign non institution other percentage\"{custom_indicator_52652_end} +  {custom_indicator_52657_start}\"mutual funds or uti percentage +  insurance companies percentage +  others institutions percentage +  clearing members percentage +  corporate bodies percentage +  govt central or state percentage +  trusts institutes percentage +  venture capital funds percentage +  nsdl intransit percentage +  financial institutions or banks percentage\"{custom_indicator_52657_end} +  others non promoter non institution percentage )\"{custom_indicator_68140_end} as '% 1 Qtr ago', Quarterly {custom_indicator_68140_start}\"total percentage - (  total foreign promoter and group percentage +  indian promoter and group percentage +  {custom_indicator_52652_start}\"foreign institution other percentage +  foreign direct investments institutions percentage +  foreign institutional investors percentage +  foreign bank percentage +  foreign collaborators percentage +  foreign venture capital investors percentage +  foreign non institution other percentage\"{custom_indicator_52652_end} +  {custom_indicator_52657_start}\"mutual funds or uti percentage +  insurance companies percentage +  others institutions percentage +  clearing members percentage +  corporate bodies percentage +  govt central or state percentage +  trusts institutes percentage +  venture capital funds percentage +  nsdl intransit percentage +  financial institutions or banks percentage\"{custom_indicator_52657_end} +  others non promoter non institution percentage )\"{custom_indicator_68140_end} as '% Current Qtr', Quarterly {custom_indicator_68140_start}\"total percentage - (  total foreign promoter and group percentage +  indian promoter and group percentage +  {custom_indicator_52652_start}\"foreign institution other percentage +  foreign direct investments institutions percentage +  foreign institutional investors percentage +  foreign bank percentage +  foreign collaborators percentage +  foreign venture capital investors percentage +  foreign non institution other percentage\"{custom_indicator_52652_end} +  {custom_indicator_52657_start}\"mutual funds or uti percentage +  insurance companies percentage +  others institutions percentage +  clearing members percentage +  corporate bodies percentage +  govt central or state percentage +  trusts institutes percentage +  venture capital funds percentage +  nsdl intransit percentage +  financial institutions or banks percentage\"{custom_indicator_52657_end} +  others non promoter non institution percentage )\"{custom_indicator_68140_end} - 3 quarter ago {custom_indicator_68140_start}\"total percentage - (  total foreign promoter and group percentage +  indian promoter and group percentage +  {custom_indicator_52652_start}\"foreign institution other percentage +  foreign direct investments institutions percentage +  foreign institutional investors percentage +  foreign bank percentage +  foreign collaborators percentage +  foreign venture capital investors percentage +  foreign non institution other percentage\"{custom_indicator_52652_end} +  {custom_indicator_52657_start}\"mutual funds or uti percentage +  insurance companies percentage +  others institutions percentage +  clearing members percentage +  corporate bodies percentage +  govt central or state percentage +  trusts institutes percentage +  venture capital funds percentage +  nsdl intransit percentage +  financial institutions or banks percentage\"{custom_indicator_52657_end} +  others non promoter non institution percentage )\"{custom_indicator_68140_end} as 'Change in 3 Qtrs' WHERE {cash} 1 = 1 GROUP BY symbol ORDER BY 5 desc",
        "icon": "👥"
    },

    "Industry Analysis": {
        "query": "select latest \"close - 1 candle ago close / 1 candle ago close * 100\" as 'Daily', Weekly \"close - 1 candle ago close / 1 candle ago close * 100\" as 'Weekly', Monthly \"close - 1 candle ago close / 1 candle ago close * 100\" as 'Monthly', Yearly \"close - 1 candle ago close / 1 candle ago close * 100\" as '1 year', 1 year ago \"close - 1 candle ago close / 1 candle ago close * 100\" as '2 years', 2 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" as '3 years', 4 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" as '5 years', 1 year ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 2 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 3 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 4 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 5 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" / 5 as 'CAGR(5Y)', 1 year ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 2 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 3 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 4 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 5 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 6 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 7 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 8 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 9 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" + 10 years ago \"close - 1 candle ago close / 1 candle ago close * 100\" / 10 as 'CAGR(10Y)' WHERE( {cash} ( market cap > 500 ) ) GROUP BY industry ORDER BY 2 desc",
        "icon": "🏭"
    },

    "Stock List": {
        "query": "select Symbol as 'STOCK NAME', latest Close as 'PRICE', ( ( latest Close - 1 day ago Close ) / 1 day ago Close ) * 100 as '% CHANGE', 52 weeks ago High as '52w High', ( ( 52 weeks ago High - latest Close ) / 52 weeks ago High ) * 100 as 'Distance 52w High', Market Cap as 'Market Cap' WHERE {cash} 1 = 1 GROUP BY symbol ORDER BY 1 desc",
        "icon": "📋"
    },

    "1 Year Return": {
        "query": "select ( ( latest Close - 1 year ago Close ) / 1 year ago Close ) * 100 as '% change' WHERE( {cash} ( latest close > 20 and market cap > 500 ) ) GROUP BY symbol ORDER BY 1 desc",
        "icon": "📅"
    },

    "Promoters Increasing": {
        "query": "select Quarterly {custom_indicator_74292_start}\"indian promoter and group percentage + total foreign promoter and group percentage\"{custom_indicator_74292_end} as '% Current Qtr', Quarterly {custom_indicator_74292_start}\"indian promoter and group percentage + total foreign promoter and group percentage\"{custom_indicator_74292_end} - 3 quarter ago {custom_indicator_74292_start}\"indian promoter and group percentage + total foreign promoter and group percentage\"{custom_indicator_74292_end} as 'Change in 3 Qtrs', 3 quarter ago {custom_indicator_74292_start}\"indian promoter and group percentage + total foreign promoter and group percentage\"{custom_indicator_74292_end} as '% 3 Qtr ago', 2 quarter ago {custom_indicator_74292_start}\"indian promoter and group percentage + total foreign promoter and group percentage\"{custom_indicator_74292_end} as '% 2 Qtr ago', 1 quarter ago {custom_indicator_74292_start}\"indian promoter and group percentage + total foreign promoter and group percentage\"{custom_indicator_74292_end} as '% 1 Qtr ago' WHERE {cash} 1 = 1 GROUP BY symbol ORDER BY 2 desc",
        "icon": "👔"
    },

    "2 Week Return": {
        "query": "select ( ( latest Close - 14 days ago Close ) / 14 days ago Close ) * 100 as '% change' WHERE( {cash} ( latest close > 20 and market cap > 500 ) ) GROUP BY symbol ORDER BY 1 desc",
        "icon": "📆"
    },

    "FII Increasing Stake": {
        "query": "select 3 quarter ago {custom_indicator_52652_start}\"foreign institution other percentage + foreign direct investments institutions percentage + foreign institutional investors percentage + foreign bank percentage + foreign collaborators percentage + foreign venture capital investors percentage + foreign non institution other percentage\"{custom_indicator_52652_end} as '% 3 Qtr ago', 2 quarter ago {custom_indicator_52652_start}\"foreign institution other percentage + foreign direct investments institutions percentage + foreign institutional investors percentage + foreign bank percentage + foreign collaborators percentage + foreign venture capital investors percentage + foreign non institution other percentage\"{custom_indicator_52652_end} as '% 2 Qtr ago', 1 quarter ago {custom_indicator_52652_start}\"foreign institution other percentage + foreign direct investments institutions percentage + foreign institutional investors percentage + foreign bank percentage + foreign collaborators percentage + foreign venture capital investors percentage + foreign non institution other percentage\"{custom_indicator_52652_end} as '% 1 Qtr ago', Quarterly {custom_indicator_52652_start}\"foreign institution other percentage + foreign direct investments institutions percentage + foreign institutional investors percentage + foreign bank percentage + foreign collaborators percentage + foreign venture capital investors percentage + foreign non institution other percentage\"{custom_indicator_52652_end} as '% Current Qtr', Quarterly {custom_indicator_52652_start}\"foreign institution other percentage + foreign direct investments institutions percentage + foreign institutional investors percentage + foreign bank percentage + foreign collaborators percentage + foreign venture capital investors percentage + foreign non institution other percentage\"{custom_indicator_52652_end} - 3 quarter ago {custom_indicator_52652_start}\"foreign institution other percentage + foreign direct investments institutions percentage + foreign institutional investors percentage + foreign bank percentage + foreign collaborators percentage + foreign venture capital investors percentage + foreign non institution other percentage\"{custom_indicator_52652_end} as 'Change in 3 Qtr' WHERE {cash} 1 = 1 GROUP BY symbol ORDER BY 5 desc",
        "icon": "🌍"
    },
}


def fetch_from_chartink(payload, timeout=15):
    """Fetch from Chartink"""
    with requests.Session() as s:
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

        warm = s.get(CI_HOME, timeout=timeout)
        scr = s.get(CI_SCREENER, timeout=timeout)
        scr.raise_for_status()

        soup = bs(scr.content, "lxml")
        meta = soup.find("meta", {"name": "csrf-token"})
        if not meta or not meta.get("content"):
            raise RuntimeError("CSRF token not found")
        csrf = meta["content"]

        headers = {
            "X-CSRF-TOKEN": csrf,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
            "Origin": CI_HOME,
            "Referer": scr.url,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        
        resp = s.post(CI_WIDGET_PROCESS, data=payload, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()


def parse_widget_data(data):
    """Parse Chartink response"""
    if "groupData" not in data:
        return None
    
    rows = []
    for group in data["groupData"]:
        stock_name = group.get("name", "")
        results = group.get("results", [])
        
        row = {"Stock": stock_name}
        for result_dict in results:
            for key, values in result_dict.items():
                if isinstance(values, list) and values:
                    clean_val = values[-1] if values[-1] != 1.7e+308 else "N/A"
                    row[key.title()] = clean_val
                else:
                    row[key.title()] = values
        rows.append(row)
    
    return pd.DataFrame(rows)


def is_market_open():
    """Check if Indian stock market is open"""
    now = datetime.now(IST)
    
    # Check if weekend (Saturday=5, Sunday=6)
    if now.weekday() >= 5:
        return False
    
    # Market hours: 9:15 AM - 3:30 PM IST
    market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    return market_start <= now <= market_end


def upload_to_github():
    """Upload Excel to GitHub - EASY METHOD!"""
    try:
        print("📤 Uploading to GitHub...")
        
        # Read file as base64
        with open(EXCEL_FILE, 'rb') as f:
            content = base64.b64encode(f.read()).decode('utf-8')
        
        # GitHub API URL
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{EXCEL_FILE}"
        
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Check if file exists (to get SHA)
        sha = None
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                sha = resp.json()['sha']
        except:
            pass
        
        # Prepare data
        data = {
            "message": f"Update data: {datetime.now(IST).strftime('%Y-%m-%d %H:%M')}",
            "content": content,
            "branch": GITHUB_BRANCH
        }
        
        if sha:
            data["sha"] = sha  # Update existing file
        
        # Upload
        response = requests.put(url, json=data, headers=headers)
        
        if response.status_code in [200, 201]:
            download_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{EXCEL_FILE}"
            print(f"✅ Uploaded to GitHub!")
            print(f"🔗 Download URL: {download_url}")
            return download_url
        else:
            print(f"❌ Upload failed: {response.status_code}")
            print(f"   {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ GitHub upload error: {e}")
        return None


def update_excel_file():
    """Fetch and save to Excel"""
    print(f"\n{'='*70}")
    print(f"🔄 {datetime.now(IST).strftime('%d %b %Y, %I:%M %p')}")
    print(f"{'='*70}")
    
    success_count = 0
    
    try:
        with pd.ExcelWriter(EXCEL_FILE, engine='openpyxl') as writer:
            
            # Metadata
            metadata_df = pd.DataFrame({
                'Last Updated': [datetime.now(IST).strftime('%d %b %Y, %I:%M %p')],
                'Total Queries': [len(QUERIES)],
            })
            metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
            print("✅ Metadata")
            
            # Fetch queries
            for i, (query_name, query_info) in enumerate(QUERIES.items(), 1):
                print(f"\n📊 [{i}/{len(QUERIES)}] {query_name}")
                
                try:
                    payload = {"query": query_info["query"]}
                    data = fetch_from_chartink(payload)
                    df = parse_widget_data(data)
                    
                    if df is not None and not df.empty:
                        df.insert(0, 'Icon', query_info['icon'])
                        df.to_excel(writer, sheet_name=query_name[:31], index=False)
                        print(f"   ✅ {len(df)} rows")
                        success_count += 1
                    else:
                        print(f"   ⚠️  No data")
                        
                except Exception as e:
                    print(f"   ❌ {e}")
                
                time.sleep(2)
        
        print(f"\n✅ Excel saved!")
        
        # Upload to GitHub
        if success_count > 0:
            upload_to_github()
        
        return True
        
    except Exception as e:
        print(f"❌ {e}")
        return False


def main():
    """Main loop"""
    print("🚀 Market Health Fetcher - GitHub Edition")
    print(f"📊 Queries: {len(QUERIES)}")
    print(f"⏱️  Interval: {UPDATE_INTERVAL}s")
    print(f"🕐 Market Hours: 9:15 AM - 3:30 PM IST (Mon-Fri)\n")
    
    while True:
        try:
            # Check if market is open
            if is_market_open():
                print("✅ Market is OPEN - Fetching data...")
                update_excel_file()
                print(f"\n⏳ Next update in {UPDATE_INTERVAL}s...\n")
            else:
                now = datetime.now(IST)
                print(f"⏸️  Market CLOSED ({now.strftime('%I:%M %p')}) - Skipping update")
                
                # Calculate time until market opens
                if now.weekday() >= 5:  # Weekend
                    days_until_monday = 7 - now.weekday()
                    next_open = now + pd.Timedelta(days=days_until_monday)
                    next_open = next_open.replace(hour=9, minute=15, second=0)
                    print(f"   Next market open: Monday at 9:15 AM")
                elif now.hour < 9 or (now.hour == 9 and now.minute < 15):
                    print(f"   Market opens today at 9:15 AM")
                else:
                    print(f"   Market opens tomorrow at 9:15 AM")
                
                print(f"   Checking again in {UPDATE_INTERVAL}s...\n")
            
            time.sleep(UPDATE_INTERVAL)
            
        except KeyboardInterrupt:
            print("\n🛑 Stopped")
            break
        except Exception as e:
            print(f"❌ {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
