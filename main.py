from tvDatafeed import TvDatafeed, Interval
import yfinance as yf
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
today = datetime.today()
# 60 gün önceki tarihi hesapla
sixty_days_ago_ex = today - timedelta(days=59)
seven_hundred_thirty_days_ago_ex = today- timedelta(days=730)
# Tarihi belirtilen formatta yazdır
sixty_days_ago = sixty_days_ago_ex.strftime("%Y-%m-%d")
seven_hundred_thirty_days_ago =seven_hundred_thirty_days_ago_ex.strftime("%Y-%m-%d")
# Veritabanı bağlantı bilgileri
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "",
    "database": "fonsite"
}
tv = TvDatafeed()

TIMEFRAMES = [ "5m","15m","30m","1h","1d", "5d"]  #
ticker = "AAPL"
host= "127.0.0.1"
user= "root"
password= ""
database= "fonsite"
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
def create_table_if_not_exists(cursor, table_name):
    """
    Eğer tablo mevcut değilse oluşturur.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        date DATETIME PRIMARY KEY,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        adj_close FLOAT,
        volume BIGINT
    );
    """
    cursor.execute(create_table_query)

def insert_data(cursor, table_name, data):
    """
    Yeni verileri veritabanına ekler.
    """
    insert_query = f"""
    INSERT IGNORE INTO {table_name} (date, open, high, low, close, adj_close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    cursor.executemany(insert_query, data)

def fetch_existing_dates(cursor, table_name):
    """
    Veritabanındaki mevcut tarihleri alır.
    """
    cursor.execute(f"SELECT date FROM {table_name};")
    return set(row[0] for row in cursor.fetchall())

def fetch_data(symbol, timeframe):
    """
    yfinance'dan veriyi çeker.
    """
    try:
        if timeframe == '5m':
            df = yf.download(symbol,period="5d", interval=timeframe)
        elif timeframe == '15m':
            df = yf.download(symbol, period="15d", interval=timeframe)
        elif timeframe == '30m':
            df = yf.download(symbol,period="60d", interval=timeframe)
        elif timeframe == '1h':
            df = yf.download(symbol,period="730d", interval=timeframe)
        else:
            df = yf.download(symbol,period="5d", interval=timeframe)
        
        df.reset_index(inplace=True)
        df.columns = [' '.join(col).strip() if isinstance(col, tuple) else col for col in df.columns]
        return df
    except Exception as e:
        print(f"Error fetching data for {timeframe}: {e}")
        return None

def tablenamegenerator(timeframe):
    """ Gelen timeframe'e göre table adı döndürür"""
    if timeframe == Interval.in_1_minute:
        return "1m"
    if timeframe == Interval.in_3_minute:
        return "3m"
    if timeframe == Interval.in_5_minute:
        return "5m"
    if timeframe == Interval.in_15_minute:
        return "15m"
    if timeframe == Interval.in_30_minute:
        return "30m"
    if timeframe == Interval.in_45_minute:
        return "45m"
    if timeframe == Interval.in_1_hour:
        return "1h"
    if timeframe == Interval.in_2_hour:
        return "2h"
    if timeframe == Interval.in_3_hour:
        return "3h"
    if timeframe == Interval.in_4_hour:
        return "4h"
    if timeframe == Interval.in_daily:
        return "1d"
    if timeframe == Interval.in_weekly:
        return "1wk"
    if timeframe == Interval.in_monthly:
        return "1mo"
    if timeframe == Interval.in_yearly:
        return "1y"
        

def process_timeframe(conn, symbol, timeframe):
    """
    Belirli bir zaman aralığı için veriyi işler.
    """
    cursor = conn.cursor()
    tabletime= tablenamegenerator(timeframe)
    table_name = f"data_{symbol}_{tabletime}"
    table_name= table_name.lower()
    # Tabloyu oluştur
    #create_table_if_not_exists(cursor, table_name)
    
    # Veriyi çek
    #df = fetch_data(symbol, timeframe)
    df = tv.get_hist(symbol=symbol,interval=timeframe,n_bars=50000)
    df.reset_index(inplace=True)
    
    if df is None or df.empty:
        print(f"No data fetched for timeframe {timeframe}.")
        return

    # Veritabanındaki mevcut tarihler
    #existing_dates = fetch_existing_dates(cursor, table_name)

    # Yeni verileri seç
    
    """dfout = pd.read_sql(sql=table_name,con=engine)
    df_cleaned= pd.concat([dfout,df])
    df_cleaned = df_cleaned.drop_duplicates(subset='Datetime',keep='first')
    df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None)
    dfout['Datetime'] = dfout['Datetime'].dt.tz_localize(None)
    merged = pd.merge(df, dfout, on=['datetime', 'open','high','low','close', 'volume'], how='left', indicator=True)
    df_diff = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)
    df_diff['Datetime'] = df_diff['Datetime'].dt.tz_localize('Europe/Istanbul')
    print(df_diff)"""
   
   
    try:
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f"{table_name} tablosu oluşturuldu ve yazıldı.")
    except Exception as e:
        print(f"Bir hata oluştu: {e}")
   
    conn.commit()
        

    cursor.close()

def main():
    # MySQL bağlantısı
    conn = mysql.connector.connect(**DB_CONFIG)
    SYMBOLS = ['BIST:OPX30','BIST:APLIB','BIST:APBDL','BIST:GLDTR'] 
    # Her zaman aralığını işle
    for symbol in SYMBOLS:
        for timeframe in Interval:
            print(f"Processing timeframe: {timeframe}")
            process_timeframe(conn,symbol, timeframe)

    # Bağlantıyı kapat
    conn.close()

if __name__ == "__main__":
    main()
