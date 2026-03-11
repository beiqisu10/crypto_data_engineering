import requests
import pandas as pd

def fetch_fear_greed_csv():
    """
    Fetches the "Crypto Fear & Greed Index" from Alternative.me API.
    This provides a daily sentiment score for the market.
    """
    
    # --- 1. API Configuration ---
    # Fetching the last 31 days to ensure coverage of a full month
    url = "https://api.alternative.me/fng/?limit=31"
    
    # Executing GET request and parsing JSON response
    r = requests.get(url).json()
    
    # --- 2. Data Transformation (Pandas) ---
    # Converting the 'data' list from the JSON response into a DataFrame
    df = pd.DataFrame(r['data'])
    
    # Convert the Unix timestamp (seconds) to a readable Date format (YYYY-MM-DD)
    # Using .dt.date to discard the time component for daily aggregation
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='s').dt.date
    
    # Ensure the sentiment 'value' (0-100) is stored as an integer for calculation
    df['value'] = df['value'].astype(int)
    
    # --- 3. Feature Selection & Output ---
    # Selecting only the essential columns for our downstream dbt models:
    # - timestamp: The date of the sentiment reading
    # - value: The numeric score (0-100)
    # - value_classification: The label (e.g., "Extreme Fear", "Greed")
    df = df[['timestamp', 'value', 'value_classification']]
    
    # Return the processed data as a CSV string (excluding the index)
    # This format is optimized for GCS ingestion and BigQuery loading
    return df.to_csv(index=False)