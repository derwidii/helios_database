import streamlit as st
import pandas as pd
import plotly.express as px
from mysql.connector import connect, Error
from datetime import datetime
from io import StringIO
import os

connection_config = {
    "host": st.secrets["DB_HOST"],
    "user": st.secrets["DB_USER"],
    "password": st.secrets["DB_PASSWORD"],
    "database": st.secrets["DB_NAME"],
}



if "selected_config_id" not in st.session_state:
    st.session_state["selected_config_id"] = None

if "selected_sensors" not in st.session_state:
    st.session_state["selected_sensors"] = []

if "start_time" not in st.session_state:
    st.session_state["start_time"] = ""

if "end_time" not in st.session_state:
    st.session_state["end_time"] = ""

if "show_plot" not in st.session_state:
    st.session_state["show_plot"] = False  # Flag to control plot rendering


# Function to Fetch Data from the Database
@st.cache_data(hash_funcs={connect: id}, show_spinner=False)
def fetch_data(query):
    """Fetch data from the database based on the SQL query."""
    try:
        with connect(**connection_config) as connection:
            return pd.read_sql(query, connection)
    except Error as e:
        st.error(f"Error while connecting to MySQL: {e}")
        return pd.DataFrame()


@st.cache_data(hash_funcs={connect: id}, show_spinner=False)
def get_sensor_values_time_range(sensor_id, start_time, end_time):
    """Fetch sensor values for a specific sensor_id within a specified time range."""
    query = f"""
    SELECT value, timestamp
    FROM sensor_values
    WHERE sensor_id = '{sensor_id}' AND timestamp BETWEEN UNIX_TIMESTAMP('{start_time}') AND UNIX_TIMESTAMP('{end_time}');
    """
    df = fetch_data(query)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")  # Convert to datetime
    return df


# Function to Fetch Distinct Config IDs
def get_config_ids_with_dates():
    """Fetch distinct config IDs with dates."""
    query = "SELECT config_id, date FROM tests ORDER BY date DESC;"
    df = fetch_data(query)
    if not df.empty:
        # Combine config_id and date in a string for the dropdown
        df["config_id_date"] = df.apply(
            lambda row: f"{row['config_id']} - {row['date']}", axis=1
        )
    return df


def get_test_time_range(config_id):
    """Fetch the start and end times for a given test configuration."""
    # This query should be adapted to your database schema
    # Assuming 'sensor_values' has the necessary timestamps for simplicity
    query = f"""
    SELECT MIN(timestamp) AS start_time, MAX(timestamp) AS end_time
    FROM sensor_values
    JOIN sensors ON sensor_values.sensor_id = sensors.id
    WHERE sensors.config_id = '{config_id}';
    """
    df = fetch_data(query)
    if not df.empty:
        # Convert timestamps to your desired format
        start_time = pd.to_datetime(df.iloc[0]["start_time"], unit="s").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = pd.to_datetime(df.iloc[0]["end_time"], unit="s").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return start_time, end_time
    else:
        return "", ""


# Function to Fetch Distinct Sensor Names
def get_sensors_with_data(config_id):
    """Fetch distinct sensor names that have data for the given config_id."""
    query = f"""
    SELECT DISTINCT sensors.name 
    FROM sensors
    JOIN sensor_values ON sensors.id = sensor_values.sensor_id
    WHERE sensors.config_id = '{config_id}';
    """
    return fetch_data(query)


# Function to Fetch Sensor ID
# Function to Fetch Sensor IDs
def get_sensor_ids(sensor_names, config_id):
    """Fetch the sensor_ids for the selected sensor names and config_id."""
    sensor_ids = []
    for sensor_name in sensor_names:
        query = f"""
        SELECT id FROM sensors
        WHERE name = '{sensor_name}' AND config_id = '{config_id}';
        """
        df = fetch_data(query)
        if not df.empty:
            sensor_ids.append(df["id"].iloc[0])
    return sensor_ids


# Function to Fetch Sensor Values
# This function remains mostly the same, but you might want to include the sensor name in the DataFrame for clarity
def get_sensor_values_with_ma_for_multiple_sensors(
    sensor_ids, sensor_names, start_time=None, end_time=None
):
    dfs = []
    for sensor_id, sensor_name in zip(sensor_ids, sensor_names):
        if start_time and end_time:
            df = get_sensor_values_time_range(sensor_id, start_time, end_time)
        else:
            df = get_sensor_values(sensor_id)
        if not df.empty:
            df = calculate_moving_average(df)
            df["sensor_name"] = sensor_name  # Add sensor name to the DataFrame
            dfs.append(df)
    return pd.concat(dfs)


def get_sensor_values(sensor_id):
    """Fetch all sensor values for a specific sensor_id."""
    query = f"""
    SELECT value, timestamp
    FROM sensor_values
    WHERE sensor_id = '{sensor_id}';
    """
    df = fetch_data(query)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"], unit="s"
        )  # Convert timestamps to datetime objects
    return df


def convert_df_to_csv(df):
    """Convert dataframe to CSV string."""
    csv = StringIO()
    df.to_csv(csv, index=False)
    csv.seek(0)
    return csv.getvalue()


# Function to Calculate Moving Average
@st.cache_data
def calculate_moving_average(df, window=100):
    """Calculate the moving average of the 'value' column, using a specified window size."""
    df["value_ma"] = df["value"].rolling(window=window).mean()
    df["value_ma"] = df["value_ma"].fillna(
        df["value"]
    )  # Replace NA values with the original values for the initial window
    return df


@st.cache_data(show_spinner=False, hash_funcs={pd.DataFrame: lambda _: None})
def get_sensor_values_with_ma(sensor_id, start_time=None, end_time=None):
    """Fetch all sensor values for a specific sensor_id and optionally apply time range filtering."""
    if start_time and end_time:
        df = get_sensor_values_time_range(sensor_id, start_time, end_time)
    else:
        df = get_sensor_values(sensor_id)
    df = calculate_moving_average(df)
    return df


# Streamlit Interface Setup
st.image(
    "https://raw.githubusercontent.com/derwidii/helios_database/main/HELIOS.png",
    width=100,
)
st.title("HELIOS Testing Database Visualisation")
st.text("App Creation Date: 2024-03-13, Updated: 2024-04-10")

if "start_time" not in st.session_state:
    st.session_state["start_time"] = ""
if "end_time" not in st.session_state:
    st.session_state["end_time"] = ""

# Config ID Selection
config_id_date_options = get_config_ids_with_dates()
if not config_id_date_options.empty:
    selected_config_id_date = st.selectbox(
        "Select a Config ID with Date:",
        config_id_date_options["config_id_date"],
        key="config_id_select",
    )
    st.session_state.selected_config_id = selected_config_id_date.split(" - ")[0]

# Sensor Selection with Data Availability Check
if st.session_state.selected_config_id:
    sensor_options = get_sensors_with_data(st.session_state.selected_config_id)
    selected_sensors = st.multiselect(
        "Select Sensors:", sensor_options["name"], key="sensor_select"
    )
    st.session_state.selected_sensors = selected_sensors

    if selected_sensors:
        if "start_time" not in st.session_state or not st.session_state["start_time"]:
            start_time, end_time = get_test_time_range(
                st.session_state.selected_config_id
            )
            st.session_state["start_time"] = start_time
            st.session_state["end_time"] = end_time


        st.session_state.start_time = st.text_input(
            "Start Time (YYYY-MM-DD HH:MM:SS)",
            value=st.session_state["start_time"],
            key="start_time_input",
        )
        st.session_state.end_time = st.text_input(
            "End Time (YYYY-MM-DD HH:MM:SS)",
            value=st.session_state["end_time"],
            key="end_time_input",
        )

        if st.button("Show for Start-End-Time"):
            sensor_ids = get_sensor_ids(
                st.session_state.selected_sensors, st.session_state.selected_config_id
            )
            df_filtered = get_sensor_values_with_ma_for_multiple_sensors(
                sensor_ids,
                st.session_state.selected_sensors,
                st.session_state.start_time,
                st.session_state.end_time,
            )

            if not df_filtered.empty:
                fig = px.line(
                    df_filtered,
                    x="timestamp",
                    y="value_ma",
                    color="sensor_name",
                    title="Filtered Sensor Data",
                    labels={
                        "value_ma": "Sensor Value (Moving Avg)",
                        "timestamp": "Timestamp",
                    },
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.error("No data found for the selected range.")
