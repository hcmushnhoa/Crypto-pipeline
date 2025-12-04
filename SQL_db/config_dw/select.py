import duckdb
path='/mnt/d/learn/DE/Semina_project/datawarehouse.duckdb'
con = duckdb.connect(path)
print(con.sql("SELECT COUNT(1) FROM fact_trades"))
print(con.sql("SELECT COUNT(1) FROM fact_order_books"))
print(con.sql("SELECT COUNT(1) FROM fact_funding_rate"))
print(con.sql("SELECT COUNT(1) FROM fact_mark_price"))
con.close()