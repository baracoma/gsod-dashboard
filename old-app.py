import streamlit as st
import duckdb
import pandas as pd

DB_PATH = './gsod-ph.db'

def get_connection():
    return duckdb.connect(DB_PATH)

# Sidebar station selection
with get_connection() as con:
    stations_df = con.execute("SELECT DISTINCT station_id FROM gsod_daily ORDER BY station_id").df()

station_id = st.sidebar.selectbox("Select Station", stations_df['station_id'])

# Date range for that station
with get_connection() as con:
    min_date, max_date = con.execute(f"""
        SELECT MIN(date), MAX(date) FROM gsod_daily WHERE station_id = '{station_id}'
    """).fetchone()

date_range = st.sidebar.date_input("Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

# Query data based on selection
with get_connection() as con:
    query = f"""
        SELECT date, PRCP_mm, TEMP_C
        FROM gsod_daily
        WHERE station_id = '{station_id}'
          AND date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
        ORDER BY date
    """
    df = con.execute(query).df()

st.title(f"GSOD Dashboard – {station_id}")
st.write(f"Showing data from {date_range[0]} to {date_range[1]}")

if not df.empty:
    st.line_chart(df.set_index('date')[['TEMP_C', 'PRCP_mm']])
else:
    st.write("No data available for the selected date range.")
    
st.dataframe(df)
