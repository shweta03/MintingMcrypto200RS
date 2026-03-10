import ccxt
import pandas as pd
import time
import gspread
from google.oauth2.service_account import Credentials
import json
import os

def get_binance_rs_scaled(top_n=200):
    print("Connecting to Binance...")
    exchange = ccxt.binance({'enableRateLimit': True})
    
    markets = exchange.load_markets()
    tickers = exchange.fetch_tickers()
    
    usdt_tickers = {
        symbol: data for symbol, data in tickers.items() 
        if symbol.endswith('/USDT') and symbol in markets and markets[symbol]['active']
    }
    
    sorted_tickers = sorted(usdt_tickers.items(), key=lambda x: x[1]['quoteVolume'], reverse=True)
    top_symbols = [x[0] for x in sorted_tickers[:top_n]]
    
    results = []
    for symbol in top_symbols:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            if len(df) < 91: continue 
                
            current_close = df['close'].iloc[-1]
            close_1d = df['close'].iloc[-2]
            close_1w = df['close'].iloc[-8]
            close_1m = df['close'].iloc[-31]
            close_3m = df['close'].iloc[-91]
            
            ret_1d = (current_close - close_1d) / close_1d
            ret_1w = (current_close - close_1w) / close_1w
            ret_1m = (current_close - close_1m) / close_1m
            ret_3m = (current_close - close_3m) / close_3m
            
            raw_rs = (ret_1d * 0.3) + (ret_1w * 0.3) + (ret_1m * 0.2) + (ret_3m * 0.2)
            
            results.append({
                'Symbol': symbol,
                'Raw_RS': raw_rs, 
                '1D_Ret%': round(ret_1d * 100, 2),
                '1W_Ret%': round(ret_1w * 100, 2),
                '1M_Ret%': round(ret_1m * 100, 2),
                '3M_Ret%': round(ret_3m * 100, 2)
            })
            time.sleep(0.05) 
        except Exception as e:
            continue

    rs_df = pd.DataFrame(results)
    if rs_df.empty: return rs_df

    min_rs = rs_df['Raw_RS'].min()
    max_rs = rs_df['Raw_RS'].max()
    
    rs_df['RS_Score'] = ((rs_df['Raw_RS'] - min_rs) / (max_rs - min_rs)) * 100
    rs_df['RS_Score'] = rs_df['RS_Score'].round(2)
    
    rs_df = rs_df.drop(columns=['Raw_RS']).sort_values(by='RS_Score', ascending=False).reset_index(drop=True)
    cols = ['Symbol', 'RS_Score', '1D_Ret%', '1W_Ret%', '1M_Ret%', '3M_Ret%']
    return rs_df[cols]

if __name__ == "__main__":
    ranked_coins = get_binance_rs_scaled(top_n=200)
    
    if not ranked_coins.empty:
        print("Authenticating with Google Sheets...")
        # 1. Load the secret JSON key from GitHub Actions
        creds_json = os.environ.get('GCP_CREDENTIALS')
        creds_dict = json.loads(creds_json)
        
        # 2. Connect to Google Drive/Sheets
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        
        # 3. Open the sheet and update it
        # CHANGE THIS NAME if you named your Google Sheet something different!
        sheet = client.open("Crypto_RS_Live").sheet1 
        
        print("Clearing old data and uploading fresh RS rankings...")
        sheet.clear()
        sheet.update([ranked_coins.columns.values.tolist()] + ranked_coins.values.tolist())
        print("✅ Google Sheet successfully updated!")
