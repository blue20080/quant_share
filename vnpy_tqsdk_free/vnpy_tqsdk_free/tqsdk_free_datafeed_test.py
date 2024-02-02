# 配置好vt_setting.json里的datafeed字段后，运行测试
from vnpy_tqsdk_free.tqsdk_free_datafeed import TqsdkDatafeed
from vnpy.trader.object import HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from datetime import datetime, timedelta
import mysql.connector

db_config = {
    'host': '43.139.44.45',
    'user': 'root',
    'password': 'c8722dbf70cae9e727d15907af4f7960',
    'database': 'vnpy'
}


test_day_len = 365*5
symbol_vnpy = "rb60min"
exchange = "SHFE"
interval_vnpy = "1m"
def main():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    data_feed = TqsdkDatafeed()
    req = HistoryRequest(symbol="888rb",
                         exchange=Exchange.SHFE,
                         start=datetime.now() - timedelta(days=test_day_len),
                         end=datetime.now(),
                         interval=Interval.MINUTE)

    bars = data_feed.query_bar_history(req=req)


    for bar in bars:
        insert_sql = "insert IGNORE into vnpy.dbbardata (symbol, exchange, datetime, `interval`, volume, turnover, open_interest, open_price," \
                     "  high_price, low_price, close_price) " \
                     "  values (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s);"
        values = (symbol_vnpy, exchange, bar.datetime, interval_vnpy,
                  bar.volume, bar.turnover, bar.open_interest, bar.open_price, bar.high_price, bar.low_price, bar.close_price)
        print(values)
        cursor.execute(insert_sql, values)

    update_sql_start = f"update vnpy.dbbaroverview set start = (select datetime " \
             " from vnpy.dbbardata where symbol = %s and `interval` = %s " \
             " order by datetime asc limit 1) where symbol = %s and `interval` = %s"

    update_sql_end = f"update vnpy.dbbaroverview set end = (select datetime " \
                 " from vnpy.dbbardata where symbol = %s and `interval` = %s " \
                 " order by datetime desc limit 1) where symbol = %s and `interval` = %s"

    count_sql = f"update vnpy.dbbaroverview set count = (select count(*) " \
                 " from vnpy.dbbardata where symbol = %s and `interval` = %s " \
                 " order by datetime desc limit 1) where symbol = %s and `interval` = %s"

    values = (symbol_vnpy, interval_vnpy, symbol_vnpy, interval_vnpy)
    cursor.execute(update_sql_start, values)
    cursor.execute(update_sql_end, values)
    cursor.execute(count_sql,values)
    # 获取受影响的行数
    print("受影响的行数:", cursor.rowcount)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
