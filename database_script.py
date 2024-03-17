import streamlit as st
import pandas as pd
import plotly.express as px
from mysql.connector import connect, Error
from datetime import datetime
from io import StringIO
import os

connection_config = {
        "host": os.environ.get("DB_HOST"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "database": os.environ.get("DB_NAME"),
    }



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


# Function to Fetch Distinct Sensor Names
def get_sensors():
    """Fetch distinct sensor names."""
    query = "SELECT DISTINCT name FROM sensors;"
    return fetch_data(query)


# Function to Fetch Sensor ID
def get_sensor_id(sensor_name, config_id):
    """Fetch the sensor_id for the selected sensor name and config_id."""
    query = f"""
    SELECT id FROM sensors
    WHERE name = '{sensor_name}' AND config_id = '{config_id}';
    """
    df = fetch_data(query)
    return df["id"].iloc[0] if not df.empty else None


# Function to Fetch Sensor Values
def get_sensor_values(sensor_id):
    """Fetch all sensor values for a specific sensor_id."""
    query = f"""
    SELECT value, timestamp
    FROM sensor_values
    WHERE sensor_id = '{sensor_id}';
    """
    df = fetch_data(query)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")  # Convert to datetime
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


# Streamlit Interface
st.image("https://raw.githubusercontent.com/derwidii/helios_database/main/HELIOS.png", width=100)

st.text("DB_HOST:", os.environ.get("DB_HOST"))
st.title("HELIOS Testing Database Visualisation")
st.text("App Creation Date: 2024-03-13")

# Config ID Selection with Dates
config_id_date_options = get_config_ids_with_dates()
selected_config_id_date = st.selectbox(
    "Select a Config ID with Date:", config_id_date_options["config_id_date"]
)
selected_config_id = selected_config_id_date.split(" - ")[0]

# Sensor Selection
sensor_options = get_sensors()
selected_sensor = st.selectbox("Select a Sensor:", sensor_options["name"])

# Display data range and allow for time range selection
if selected_sensor:
    sensor_id = get_sensor_id(selected_sensor, selected_config_id)
    if sensor_id:
        df_full = get_sensor_values(sensor_id)
        if not df_full.empty:
            min_date, max_date = df_full["timestamp"].min(), df_full["timestamp"].max()
            st.write(f"Data available from {min_date} to {max_date}")
            start_time = st.text_input(
                "Start Time (YYYY-MM-DD HH:MM:SS)",
                value=min_date.strftime("%Y-%m-%d %H:%M:%S"),
            )
            end_time = st.text_input(
                "End Time (YYYY-MM-DD HH:MM:SS)",
                value=max_date.strftime("%Y-%m-%d %H:%M:%S"),
            )

            if st.button("Show Filtered Plot"):
                df_filtered = get_sensor_values_with_ma(sensor_id, start_time, end_time)
                if not df_filtered.empty:
                    fig = px.line(
                        df_filtered,
                        x="timestamp",
                        y="value_ma",
                        title="Filtered Sensor Data",
                        labels={
                            "value_ma": "Sensor Value (Moving Avg)",
                            "timestamp": "Timestamp",
                        },
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # CSV Download for the filtered dataset
                    csv = convert_df_to_csv(
                        df_filtered[["timestamp", "value", "value_ma"]]
                    )
                    st.download_button(
                        label="Download filtered data as CSV",
                        data=csv,
                        file_name="filtered_sensor_data.csv",
                        mime="text/csv",
                    )
                else:
                    st.error("No data found for the selected range.")
        else:
            st.error("No data found for the selected sensor.")
