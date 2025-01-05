'''DOGRU ÇALIŞAN KOD'''
import asyncio
from tvDatafeed import TvDatafeed, Interval
import yfinance as yf
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Veritabanı bağlantı bilgileri
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "",
    "database": "fonsite"
}
# Zaman dilimlerini ve her birinin saniye cinsinden karşılıklarını belirliyoruz
TIMEFRAMES = {
    Interval.in_5_minute: 300,  # 5 dakika
    Interval.in_15_minute: 900,  # 15 dakika
    Interval.in_30_minute: 1800,  # 30 dakika
    Interval.in_1_hour: 3600,  # 1 saat
    Interval.in_daily: 86400,  # 1 gün
    Interval.in_weekly: 604800,  # 1 hafta
    Interval.in_monthly: 2592000  # 1 ay
}
# İşlem yapılacak semboller
SYMBOLS = [
    'NASDAQ:QQQ', 'NASDAQ:GOOG', 'BIST:ZTM25', 'BIST:ALTIN', 'BIST:USDTR',
    'BIST:GMSTR', 'BIST:ZSR25', 'BIST:ZRE20', 'BIST:ZPX30', 'BIST:ZPT10',
    'BIST:ZPLIB', 'BIST:ZPBDL', 'BIST:ZGOLD', 'BIST:ZELOT', 'BIST:Z30KP',
    'BIST:Z30KE', 'BIST:Z30EA', 'BIST:QTEMZ', 'BIST:OPX30', 'BIST:ISGLK',
    'BIST:GLDTR', 'BIST:APX30', 'BIST:APLIB', 'BIST:APBDL'
]
# SQLAlchemy motoru ile veritabanı bağlantısı kuruyoruz
engine = create_engine(f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
# Verilen zaman dilimine göre tablo adını döndüren fonksiyon
def tablenamegenerator(timeframe):
    """ Gelen timeframe'e göre table adı döndürür"""
    timeframes_map = {
        Interval.in_1_minute: "1m",
        Interval.in_3_minute: "3m",
        Interval.in_5_minute: "5m",
        Interval.in_15_minute: "15m",
        Interval.in_30_minute: "30m",
        Interval.in_45_minute: "45m",
        Interval.in_1_hour: "1h",
        Interval.in_2_hour: "2h",
        Interval.in_3_hour: "3h",
        Interval.in_4_hour: "4h",
        Interval.in_daily: "1d",
        Interval.in_weekly: "1wk",
        Interval.in_monthly: "1mo",
        Interval.in_yearly: "1y"
    }
    return timeframes_map.get(timeframe, "unknown")
# Zaman dilimine uygun olarak veritabanına veri ekleme veya güncelleme işlemi
def process_timeframe(conn, symbol, timeframe, tv):
    cursor = conn.cursor()
    tabletime = tablenamegenerator(timeframe) # Zaman dilimine uygun tablo adı
    table_name = f"data_{symbol}_{tabletime}".lower()# Tablo adı oluşturuluyor
    # Tablo var mı diye kontrol etme
    is_exist = False
    try:
        cursor.execute(f'SELECT * FROM `{table_name}` ORDER BY datetime DESC LIMIT 1;')
        result = cursor.fetchone()
        is_exist = True
    except Exception:
        is_exist = False
    # TradingView'dan tarihsel verileri al

    df = tv.get_hist(symbol=symbol, interval=timeframe, n_bars=300, extended_session=True)
    df.reset_index(inplace=True)
    # Tablo varsa, son veriyi kontrol et ve yeni verileri ekle
    if is_exist:
        last_date_query = f'SELECT * FROM `{table_name}` ORDER BY datetime DESC LIMIT 1;'
        dfout = pd.read_sql(sql=last_date_query, con=engine)
        dfout['datetime'] = dfout['datetime'].dt.tz_localize(None)
        last_date_exist = dfout['datetime'].iloc[0]
        df_filtered = df[df['datetime'] > last_date_exist]
        df_filtered.reset_index(drop=True, inplace=True)
        
        if df_filtered.empty:
            print(f"{table_name} is up-to-date.")
        else:
            try:
                df_filtered.to_sql(name=table_name, con=engine, if_exists="append", index=False)
                print(f"Data appended to {table_name}.")
            except Exception as e:
                print(f"Error appending data to {table_name}: {e}")
    else:
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f"Table {table_name} created and data inserted.")

    conn.commit()
    cursor.close()

async def process_timeframe_periodically(timeframe, interval):
    conn = mysql.connector.connect(**DB_CONFIG)
    tv = TvDatafeed()

    while True:
        for symbol in SYMBOLS:
            try:
                print(f"Processing {symbol} for timeframe {timeframe}")
                process_timeframe(conn, symbol, timeframe, tv) # Veriyi işleyip veritabanına ekliyoruz
            except Exception as e:
                print(f"Error processing {symbol} for {timeframe}: {e}")

        print(f"Waiting {interval} seconds for timeframe {timeframe}...")
        await asyncio.sleep(interval) # Asenkron bekleme

    conn.close()


async def main():
    tasks = []
    for timeframe, interval in TIMEFRAMES.items(): # Her bir zaman dilimi için işleme başla
        tasks.append(process_timeframe_periodically(timeframe, interval)) # Zaman dilimlerini işlemek için görev oluştur

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
