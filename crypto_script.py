import requests
from datetime import datetime, timedelta
import time
import csv
import json
from pathlib import Path

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
CSV_FILE = 'crypto_data.csv'
FIELDNAMES = ['Date', 'Coin Name', 'Price', 'Change', 'Market Cap', 'Volume']
MAX_RETRIES = 7
INITIAL_DELAY = 45  # Conservative delay for bulk historical data
SESSION = requests.Session()

def fetch_historical_data(target_date: datetime) -> list:
    params = {
        'vs_currency': 'usd',
        'order': 'volume_desc',
        'per_page': 20,
        'page': 1,
        'sparkline': 'false',
        'date': target_date.strftime("%d-%m-%Y")
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = SESSION.get(
                API_URL,
                params=params,
                headers={'User-Agent': 'Mozilla/5.0 (Historical Data Fetcher)'}
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                wait = (2 ** attempt) * INITIAL_DELAY
                print(f"⚠️ Rate limited (Attempt {attempt+1}): Waiting {wait}s")
                time.sleep(wait)
                continue
            print(f"HTTP Error: {e}")
            break
        except Exception as e:
            print(f"Request failed: {e}")
            break
            
    print(f"❌ Failed to fetch {target_date} after {MAX_RETRIES} retries")
    return []

def main():
    # Simulated "today" as 2025-03-05
    end_date = datetime(2025, 3, 5)
    start_date = end_date - timedelta(days=5*365)
    dates = [end_date - timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    
    # Load progress
    existing_dates = set()
    if Path(CSV_FILE).exists():
        with open(CSV_FILE) as f:
            reader = csv.DictReader(f)
            existing_dates = {row['Date'] for row in reader}
    
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not existing_dates:
            writer.writeheader()
            
        for date in dates:
            date_str = date.strftime('%Y-%m-%d')
            if date_str in existing_dates:
                continue
                
            print(f"🔄 Fetching {date_str}...")
            data = fetch_historical_data(date)
            
            if not data:
                continue
                
            writer.writerows([{
                'Date': date_str,
                'Coin Name': coin.get('name'),
                'Price': coin.get('current_price'),
                'Change': coin.get('price_change_percentage_24h'),
                'Market Cap': coin.get('market_cap'),
                'Volume': coin.get('total_volume')
            } for coin in data])
            
            # Conservative delay even after success
            time.sleep(INITIAL_DELAY)

if __name__ == "__main__":
    main()