import requests
import pandas as pd

def fetch_coingecko_prices_csv():
    """
    Fetches historical Bitcoin prices from CoinGecko API and returns data in CSV format.
    Target: Last 30 days of daily closing prices in USD.
    """
    
    # --- 1. API Request Configuration ---
    # URL parameters: 
    # 'days=30': historical data for the last month
    # 'interval=daily': ensures we get one data point per day (closing price)
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=30&interval=daily"
    
    # Sending GET request and parsing the JSON response
    r = requests.get(url).json()
    
    # --- 2. Data Transformation (Pandas) ---
    # The API returns 'prices' as a list of lists: [[timestamp, price], ...]
    df = pd.DataFrame(r['prices'], columns=['timestamp', 'price'])
    
    # Convert Unix timestamp (milliseconds) to readable Date format
    # .dt.date is used to remove the time component, keeping only the YYYY-MM-DD
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms').dt.date
    
    # Round the price to 2 decimal places for financial data consistency
    df['price'] = df['price'].round(2)
    
    # --- 3. Output Generation ---
    # Convert the processed DataFrame to a CSV string without the index column
    # This format is ideal for GCS uploads or Airflow XComs
    return df.to_csv(index=False)