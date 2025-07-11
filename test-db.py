import duckdb

#con = duckdb.connect('gsod-ph.db')
with duckdb.connect('gsod-ph.db') as con:
    print(con.execute("PRAGMA table_info(gsod_daily)").df())
