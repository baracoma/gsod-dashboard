import streamlit as st
import duckdb
import pandas as pd
import altair as alt

DB_PATH = './gsod_ph.db'

st.set_page_config(page_title="GSOD PH Dashboard", layout="wide")

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH)

con = get_connection()

st.title("Global Summary of the Day  Philippines Dashboard")
st.markdown("""Data is downloaded from [NOAA NCEI Global Surface Summary of the Day - GSOD](https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00516) and then converted from imperial to metric. The list of available stations along with their names and coordinates can be found [here](https://github.com/baracoma/gsod-dashboard/blob/main/preprocessors/isd-history-ph.csv).

**Disclaimer:** This dashboard is for educational and research purposes only. The data is provided as is and may contain errors and missing data. For complete, verified, and official data, please consult [PAGASA Climatology and Agrometeorology Division (CAD)](https://www.pagasa.dost.gov.ph/climate/climate-data).""")

@st.cache_data
def load_stations():
    stations_df = con.execute("""
        SELECT DISTINCT station_id, station_name, lat, lon
        FROM gsod_daily
        ORDER BY station_name
    """).df()
    stations_df['label'] = stations_df['station_id'] + " – " + stations_df['station_name']
    return stations_df

stations_df = load_stations()

variable_options = {
    'PRCP_mm': 'Precipitation',
    'RAINY_DAYS': 'Rainy Days',
    'TEMP_C': 'Mean Temperature',
    'MAX_C': 'Maximum Temperature',
    'MIN_C': 'Minimum Temperature',
    'TEMP_ANOMALY': 'Temperature Anomaly'
}

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

    year_range = list(range(min_date.year, max_date.year + 1))
    month_range = list(range(1, 13))
    
    col1, col2 = st.columns(2)
    with col1:
        start_year = st.selectbox("Start Year", year_range, index=year_range.index(min_date.year))
        end_year = st.selectbox("End Year", [y for y in year_range if y >= start_year], index=([y for y in year_range if y >= start_year].index(max_date.year) if max_date.year >= start_year else 0))
    with col2:
        start_month = st.selectbox("Start Month", month_range, index=min_date.month - 1)
        end_month = st.selectbox("End Month", month_range, index=max_date.month - 1)

    date_range = (
        pd.Timestamp(f"{start_year}-{start_month:02d}-01"),
        pd.Timestamp(f"{end_year}-{end_month:02d}-28")
    )

    plot_variable_label = st.selectbox(
        "Variable to Plot",
        list(variable_options.values()),
        key="variable_select"
    )
    plot_variable = [k for k, v in variable_options.items() if v == plot_variable_label][0]

    if plot_variable == 'TEMP_ANOMALY':
        agg_level = st.selectbox(
            "Aggregation Level", ["Daily", "Monthly", "Yearly"], key="aggregation_select_anomaly"
        )
    elif plot_variable == 'RAINY_DAYS':
        agg_level = st.selectbox(
            "Aggregation Level", ["Monthly", "Yearly"], key="aggregation_select_rainy"
        )
    else:
        agg_level = st.selectbox(
            "Aggregation Level", ["Daily", "Monthly", "Yearly", "Monthly Mean (All Years)"], key="aggregation_select"
        )

if plot_variable == "RAINY_DAYS":
    trunc_unit = 'month' if agg_level == "Monthly" else 'year'
    query = f"""
        SELECT DATE_TRUNC('{trunc_unit}', date) AS period, COUNT(*) FILTER (WHERE PRCP_mm >= 1.0) AS value
        FROM gsod_daily
        WHERE station_id = '{station_selection}'
          AND date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
        GROUP BY period
        ORDER BY period
    """
    result_df = con.execute(query).df()

elif plot_variable == "TEMP_ANOMALY":
    if agg_level == "Daily":
        group_unit = 'day'
    elif agg_level == "Monthly":
        group_unit = 'month'
    elif agg_level == "Yearly":
        group_unit = 'year'
    else:
        group_unit = 'day'  # fallback

    query = f"""
        SELECT DATE_TRUNC('{group_unit}', date) AS period, AVG(TEMP_C) AS value
        FROM gsod_daily
        WHERE station_id = '{station_selection}'
          AND date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
        GROUP BY period
        ORDER BY period
    """
    result_df = con.execute(query).df()
    period_mean = result_df['value'].mean()
    result_df['value'] = (result_df['value'] - period_mean).round(2)

else:
    if agg_level == "Daily":
        query = f"""
            SELECT date AS period, {plot_variable} AS value
            FROM gsod_daily
            WHERE station_id = '{station_selection}'
              AND date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            ORDER BY period
        """
    elif agg_level == "Monthly":
        aggregate_function = "SUM" if plot_variable == 'PRCP_mm' else "AVG"
        query = f"""
            SELECT DATE_TRUNC('month', date) AS period, {aggregate_function}({plot_variable}) AS value
            FROM gsod_daily
            WHERE station_id = '{station_selection}'
              AND date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            GROUP BY period
            ORDER BY period
        """
    elif agg_level == "Yearly":
        aggregate_function = "SUM" if plot_variable == 'PRCP_mm' else "AVG"
        query = f"""
            SELECT DATE_TRUNC('year', date) AS period, {aggregate_function}({plot_variable}) AS value
            FROM gsod_daily
            WHERE station_id = '{station_selection}'
              AND date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            GROUP BY period
            ORDER BY period
        """
    elif agg_level == "Monthly Mean (All Years)":
        if plot_variable == 'PRCP_mm':
            query = f"""
                SELECT month, AVG(monthly_total) AS value FROM (
                    SELECT EXTRACT(year FROM date) AS year, EXTRACT(month FROM date) AS month, 
                           SUM({plot_variable}) AS monthly_total
                    FROM gsod_daily
                    WHERE station_id = '{station_selection}'
                    GROUP BY year, month
                )
                GROUP BY month
                ORDER BY month
            """
        else:
            query = f"""
                SELECT EXTRACT(month FROM date) AS month, AVG({plot_variable}) AS value
                FROM gsod_daily
                WHERE station_id = '{station_selection}'
                GROUP BY month
                ORDER BY month
            """
    result_df = con.execute(query).df()




#result_df = con.execute(query).df()
if 'month' in result_df.columns:
    result_df = result_df.rename(columns={'month': 'period'})

if 'value' in result_df.columns:
    result_df['value'] = pd.to_numeric(result_df['value'], errors='coerce').round(2)

station_info = stations_df.set_index('station_id').loc[station_selection]
st.write(f"### {station_info['label']}")
st.write(f"Location: {round(float(station_info['lat']), 2)}, {round(float(station_info['lon']), 2)}")
st.write(f"Showing {plot_variable_label} aggregated {agg_level.lower()} from {date_range[0]} to {date_range[1]}.")

if not result_df.empty:
    with st.sidebar:
        if plot_variable == 'TEMP_ANOMALY':
            default_min = float(result_df['value'].min() - 1)
            default_max = float(result_df['value'].max() + 1)
        else:
            default_min = 0.0 if plot_variable in ['PRCP_mm', 'RAINY_DAYS'] else float(result_df['value'].min() - 5)
            default_max = float(result_df['value'].max() + 5)

        y_min = st.number_input("Y-axis Min", value=default_min)
        y_max = st.number_input("Y-axis Max", value=default_max)


    chart_data = result_df.set_index('period')['value'].reset_index()
    

    if plot_variable == 'PRCP_mm' or plot_variable == 'RAINY_DAYS':
        bar_size = max(10, int(300 / len(chart_data)))
        x = alt.X('period:T', title='Date') if agg_level != "Monthly Mean (All Years)" else alt.X(
            'period:O',
            sort=list(range(1, 13)),
            title='Month',
            axis=alt.Axis(labelExpr="['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][datum.value - 1]")
        )
        chart = alt.Chart(chart_data).mark_bar(size=bar_size).encode(
            # x='period:T' if agg_level != "Monthly Mean (All Years)" else 'period:O',
            x=x,
            y=alt.Y('value:Q', scale=alt.Scale(domain=[y_min, y_max]))
        )
    else:
        x = alt.X('period:T', title='Date') if agg_level != "Monthly Mean (All Years)" else alt.X(
            'period:O',
            sort=list(range(1, 13)),
            title='Month',
            axis=alt.Axis(labelExpr="['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][datum.value - 1]")
        )
        chart = alt.Chart(chart_data).mark_line().encode(
            # x='period:T' if agg_level != "Monthly Mean (All Years)" else 'period:O',
            x=x,
            y=alt.Y('value:Q', scale=alt.Scale(domain=[y_min, y_max]))
        )


    st.altair_chart(chart, use_container_width=True)
    with st.expander("Show Data Table"):
        st.dataframe(result_df)
else:
    st.warning("No data available for the selected options.")

