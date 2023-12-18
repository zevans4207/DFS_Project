# Import libraries
import psycopg2 as db
from sqlalchemy import create_engine
from db_operations_module import *
import streamlit as st
from st_aggrid import AgGrid, ColumnsAutoSizeMode, GridOptionsBuilder
from math import floor
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv(os.path.join(os.getcwd(), "config", ".env"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")


# Display title and overview section
st.markdown("# Insert Data into DFS Database")
st.markdown("## Overview")
st.markdown("Perform basic operations on the DFS database with a GUI-style menu.")

# Connect to database
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
engine_connection = engine.connect()
conn = db.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
cursor = conn.cursor()

# Display options for database operation
st.markdown("## Menu")
st.markdown("Select one of the following database operations:")
menu_choice = st.selectbox(label="Database operations", options=[
    "Get connection details",
    "List database tables",
    "Update database tables",
], index=None, label_visibility="collapsed")

if menu_choice is not None:
    # If user selects "Get connection details" display details for database connection
    if menu_choice == "Get connection details":
        st.markdown("### Connection details")
        st.dataframe(get_connection_details(conn), use_container_width=True)
    # If user selects "List database tables" display names of tables in database
    elif menu_choice == "List database tables":
        st.markdown("### Database tables")
        st.markdown("Click on a table to display more information.")
        df = pd.DataFrame(list_tables(cursor))
        builder = GridOptionsBuilder.from_dataframe(df)
        builder.configure_selection(selection_mode="single", use_checkbox=False)
        grid_options = builder.build()
        table_names = AgGrid(df, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW, gridOptions=grid_options)
        # If user selects table name display additional information
        try:
            selected_table_name = table_names["selected_rows"][0]["Table Name"]
            st.markdown(f"Choose an option for '{selected_table_name}':")
            table_choice = st.selectbox(label="Table options", options=[
                "Get record count",
                "Get first 10 records",
                "Get last 10 records"
            ], index=None, label_visibility="collapsed")
            if table_choice == "Get record count":
                st.dataframe(get_record_count(cursor, selected_table_name), use_container_width=True)
            elif table_choice == "Get first 10 records":
                st.dataframe(get_records(selected_table_name, engine_connection=engine_connection, limit=10, first=True), use_container_width=True, hide_index=True)
            elif table_choice == "Get last 10 records":
                st.dataframe(get_records(selected_table_name, engine_connection=engine_connection, limit=10, first=False), use_container_width=True, hide_index=True)
        except IndexError:
            pass
    # If user selects "Update database tables" update database with S3 bucket data
    elif menu_choice == "Update database tables":
        # Create empty containers
        msg_container = st.empty()
        checkbox_container = st.empty()
        # Confirm database update
        msg_container.warning(body="**WARNING**: Modifying database tables may impact data integrity. Proceed by checking the box below.", icon="⚠️")
        confirm_update = checkbox_container.checkbox(label="Proceed Anyway", value=False)
        if confirm_update == True:
            # Remove checkbox
            checkbox_container.empty()
            delete_records(conn, cursor)
            # While database is updating, display status widget and update timer
            with msg_container.status("Updating database. This may take a while.", expanded=False):
                # Get files in S3 bucket
                s3_files = get_s3_files()
                # List tables in database
                tables = list_tables(cursor)["Table Name"]
                # Create timer
                timer = 0
                # Get time when database update starts
                start_time = time.time()
                # Retrieve, clean, and insert files for each database table
                for table in tables:
                    # If table is "nfl_dfs_feed", "upcoming_week_salaries", or "teams", get "nfl_season_dfs_feed_tables" file group in S3 bucket
                    if table == "nfl_dfs_feed" or table == "upcoming_week_salaries" or table == "teams":
                        group_name = "nfl_season_dfs_feed_tables"
                        file_group = s3_files[group_name]
                    # Else if table is "nfl_dk_contest_standings" or "nba_dk_contest_standings" get "dk_contest_standings" file group in S3 bucket
                    elif table == "nfl_dk_contest_standings" or table == "nba_dk_contest_standings":
                        group_name = "dk_contest_standings_tables"
                        file_group = s3_files[group_name]
                    # Clean and insert data into database
                    clean_insert_data(table, group_name, file_group, engine)
                # Get time when database update is complete
                end_time = time.time()
                # Get number of seconds elapsed since database update started
                timer = floor(end_time - start_time)
            # Replace status widget with success message and print number of seconds since database update started
            msg_container.success(f"Successfully updated database! Update Time: {convert_seconds(timer)}", icon="✅")
# Close database connection
cursor.close()
conn.close()
engine.dispose()
