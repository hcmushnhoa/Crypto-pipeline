import duckdb
path='/mnt/d/learn/DE/Semina_project/datawarehouse.duckdb'
con = duckdb.connect(path)
con.execute("DROP TABLE IF EXISTS fact_trades")
con.execute("DROP TABLE IF EXISTS fact_order_books")
con.execute("DROP TABLE IF EXISTS fact_funding_rate")
con.execute("DROP TABLE IF EXISTS fact_mark_price")
con.execute("DROP TABLE IF EXISTS fact_ohlc")
con.close()