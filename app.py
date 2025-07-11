import streamlit as st
import duckdb
import pandas as pd
import altair as alt

DB_PATH = './gsod_ph.db'

st.set_page_config(page_title="GSOD PH Dashboard", layout="wide")

# Cache connection so it's reused across reruns
@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH)

con = get_connection()

st.title("GSOD Philippines Dashboard")

# Load station list with caching
@st.cache_data
def load_stations():
    stations_df = con.execute("""
        SELECT DISTINCT station_id, station_name
        FROM gsod_daily
        ORDER BY station_name
    """).df()
    stations_df['label'] = stations_df['station_id'] + " – " + stations_df['station_name']
    return stations_df

stations_df = load_stations()

# Sidebar controls
with st.sidebar:
    station_selection = st.selectbox(
        "Select Station",
        stations_df['station_id'],
        format_func=lambda x: stations_df.set_index('station_id').loc[x, 'label'],
        key="station_select"
    )

    min_date, max_date = con.execute(f"""
        SELECT MIN(date), MAX(date) FROM gsod_daily WHERE station_id = '{station_selection}'
    """).fetchone()

    date_range = st.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key="date_range"
    )

    plot_variable = st.selectbox(
        "Variable to Plot", ['TEMP_C', 'PRCP_mm'], key="variable_select"
    )

    agg_level = st.selectbox(
        "Aggregation Level", ["Daily", "Monthly", "Yearly"], key="aggregation_select"
    )

# Build query dynamically
if agg_level == "Daily":
    query = f"""
        SELECT date AS period, {plot_variable} AS value
        FROM gsod_daily
        WHERE station_id = '{station_selection}'
          AND date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
        ORDER BY period
    """
else:
    trunc_unit = 'month' if agg_level == "Monthly" else 'year'
    agg_func = 'AVG' if plot_variable == 'TEMP_C' else 'SUM'

    query = f"""
        SELECT DATE_TRUNC('{trunc_unit}', date) AS period, {agg_func}({plot_variable}) AS value
        FROM gsod_daily
        WHERE station_id = '{station_selection}'
          AND date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
        GROUP BY period
        ORDER BY period
    """

# Query and display
result_df = con.execute(query).df()

st.write(f"### {stations_df.set_index('station_id').loc[station_selection, 'label']}")
st.write(f"Showing {plot_variable} aggregated {agg_level.lower()} from {date_range[0]} to {date_range[1]}.")

if not result_df.empty:
    chart_data = result_df.set_index('period')['value'].reset_index()
    if plot_variable == 'PRCP_mm':
        chart = alt.Chart(chart_data).mark_bar().encode(
            x='period:T',
            y=alt.Y('value:Q', title='Precipitation (mm)')
        )
    else:
        chart = alt.Chart(chart_data).mark_line().encode(
            x='period:T',
            y=alt.Y('value:Q', title='Temperature (°C)', scale=alt.Scale(domain=[-5, 40]))
        )
    st.altair_chart(chart, use_container_width=True)
    with st.expander("Show Data Table"):
        st.dataframe(result_df)
else:
    st.warning("No data available for the selected options.")

