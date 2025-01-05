from tvDatafeed import TvDatafeed, Interval
import yfinance as yf
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "",
    "database": "fonsite"
}


TIMEFRAMES = [ "5m","15m","30m","1h","1d", "5d"]  #

host= "127.0.0.1"
user= "root"
password= ""
database= "fonsite"
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")


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
        

def process_timeframe(conn, symbol, timeframe,tv):
    """
    Belirli bir zaman aralığı için veriyi işler.
    """
    print("aa")
    cursor = conn.cursor()
    tabletime= tablenamegenerator(timeframe)
    table_name = f"data_{symbol}_{tabletime}"
    table_name= table_name.lower()
    # Tabloyu oluştur
    #create_table_if_not_exists(cursor, table_name)
    
    # Veriyi çek
    #df = fetch_data(symbol, timeframe)
    try:
        df = tv.get_hist(symbol=symbol,interval=timeframe,n_bars=50000)
        df.reset_index(inplace=True)
    except Exception as e:
        print(f"Data çekilirken hata oluştu: {e}")
        if df is None or df.empty:
            print(f"No data fetched for timeframe {timeframe}.")
            return
    
    dfout = pd.read_sql(sql=table_name, con=engine)

    # DataFrame'lerin birleştirilmesi ve tekrar eden kayıtların kaldırılması
    df_cleaned = pd.concat([dfout, df], ignore_index=True)
    df_cleaned = df_cleaned.drop_duplicates(subset='datetime', keep='first')

    # Zaman damgalarını zaman diliminden arındırma (timezone naive)
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce').dt.tz_localize(None)
    dfout['datetime'] = pd.to_datetime(dfout['datetime'], errors='coerce').dt.tz_localize(None)

    # DataFrame'leri birleştirme ve farkları bulma
    merged = pd.merge(
        df, 
        dfout, 
        on=['datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume'], 
        how='left', 
        indicator=True
    )

    # Sadece df'te olup dfout'ta olmayan verileri seçme
    df_diff = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    # İndeks sıfırlama
    df_diff = df_diff.reset_index(drop=True)

    # Sonuç
    print(df_diff)
    
   
    if(df_diff.empty):
        return 
    else:
        try:
            df_diff.to_sql(name=table_name, con=engine, if_exists="append", index=False)
            print(f"{table_name} tablosuna yazıldı.")
        except Exception as e:
            print(f"Tabloya eklenirken hata oluştu: {e}")
    
   
    conn.commit()
        

    cursor.close()

def main():
    # MySQL bağlantısı
    conn = mysql.connector.connect(**DB_CONFIG)
    #SYMBOLS = ['NASDAQ:QQQ','NASDAQ:GOOG','BIST:ZTM25','BIST:ALTIN','BIST:USDTR','BIST:GMSTR','BIST:ZSR25','BIST:ZRE20','BIST:ZPX30','BIST:ZPT10','BIST:ZPLIB','BIST:ZPBDL','BIST:ZGOLD','BIST:ZELOT','BIST:Z30KP','BIST:Z30KE','BIST:Z30EA','BIST:QTEMZ','BIST:OPX30','BIST:ISGLK','BIST:GLDTR','BIST:APX30','BIST:APLIB','BIST:APBDL'] 
    SYMBOLS = ['NASDAQ:QQQ','NASDAQ:GOOG','BIST:ZTM25','BIST:ALTIN','BIST:USDTR','BIST:GMSTR','BIST:ZSR25','BIST:ZRE20','BIST:ZPX30','BIST:ZPT10','BIST:ZPLIB','BIST:ZPBDL','BIST:ZGOLD','BIST:ZELOT','BIST:Z30KP','BIST:Z30KE','BIST:Z30EA','BIST:QTEMZ','BIST:OPX30','BIST:ISGLK','BIST:GLDTR','BIST:APX30','BIST:APLIB','BIST:APBDL'] 
    # Her zaman aralığını işle
    # Burada 5d değerini almadım database tüm tanımlı intervallar için alınmıştır.
    #Alttaki kod yeni bir şekilde eklenmiştir. İstenen base timeframeleri içermektedir.
    TIMES=[Interval.in_5_minute,Interval.in_15_minute,Interval.in_30_minute,Interval.in_1_hour,Interval.in_daily,Interval.in_weekly,Interval.in_monthly]
    
    for symbol in SYMBOLS:
        try:
            tv = TvDatafeed()
            for timeframe in TIMES:
                print(f"Processing timeframe: {timeframe}")
                process_timeframe(conn,symbol, timeframe,tv)
        except Exception as e:
            print(f"Bir hata oluştu: {e}")
    
    # Bağlantıyı kapat
    conn.close()

if __name__ == "__main__":
    main()
