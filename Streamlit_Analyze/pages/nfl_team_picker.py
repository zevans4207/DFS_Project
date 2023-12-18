# Import libraries
import streamlit as st, pandas as pd
from dotenv import load_dotenv
from st_aggrid import AgGrid, ColumnsAutoSizeMode, GridOptionsBuilder
import uuid
import os, sys; sys.path.append(os.path.join(os.getcwd(), ".."))
from data_analysis_module import *

# Database connection
if "engine" and "connection" not in st.session_state:
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "..", "config", ".env"))
    DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = (
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD"),
        os.getenv("DB_HOST"),
        os.getenv("DB_PORT"),
        os.getenv("DB_NAME"),
    )
    st.session_state["engine"], st.session_state["connection"] = connect_to_db(username=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, db_name=DB_NAME)
engine, connection = st.session_state["engine"], st.session_state["connection"]

if engine and connection:

    # Set page title and overview
    st.markdown("# DraftKings NFL Team Picker")
    st.header("Overview", divider="grey")
    st.markdown("Build a winning lineup with top players from the 2023 DraftKings NFL Contest Standings.")

    # Load data from database
    nfl_dfs_feed = pd.read_sql("SELECT * FROM nfl_dfs_feed;", connection)

    # Select NFL regular season week
    nfl_reg_season_week_1, nfl_reg_season_weeks = get_reg_season_weeks(sports_league="NFL", contest_dates_unique=pd.unique(nfl_dfs_feed["date"].sort_values(ascending=True)))
    if "All Weeks" in nfl_reg_season_weeks:
        nfl_reg_season_weeks.remove("All Weeks")
    st.sidebar.header("Select a Week")
    nfl_reg_season_week = st.sidebar.selectbox("Select a Week", nfl_reg_season_weeks, index=0, label_visibility="collapsed")

    # Store salary cap as session state variable
    salary_cap = f"salary_cap_{nfl_reg_season_week.lower().replace(' ', '_')}"
    if salary_cap not in st.session_state:
        st.session_state[salary_cap] = 50000

    # Filter NFL DFS Feed by selected week
    nfl_dfs_feed_filtered = nfl_dfs_feed[nfl_dfs_feed["week"] == int(nfl_reg_season_week.split(" ")[1])].copy()

    # Drop extraneous columns from NFL DFS Feed
    nfl_dfs_feed_filtered.drop(labels=["record_id", "dataset", "game_id", "start_time_et", "player_id", "opponent", "venue", "position_fd", "salary_fd", "fpts_fd", "date", "week"], axis="columns", inplace=True)

    # Rename and rearrange lineup options columns
    nfl_dfs_feed_filtered.rename(columns={"position_dk": "Position", "player_dst": "Player", "team": "Team", "salary_dk": "Salary", "fpts_dk": "Points"}, inplace=True)
    lineup_options = nfl_dfs_feed_filtered.iloc[:, [0, 2, 1, 3, 4]]

    # Display user's lineup
    st.header(body="Your Lineup", divider="grey")
    toggle_mode_state = st.radio(label="Toggle Mode", options=["Add Mode", "Delete Mode"], index=0, label_visibility="collapsed")
    salary_cap_container = st.empty()
    salary_cap_container.markdown(body=f"**Remaining Salary**: $" + "{:,}".format(st.session_state[salary_cap]))
    msg_container = st.empty()
    lineup_container = st.empty()

    # Store lineup as session state variable
    lineup = f"lineup_{nfl_reg_season_week.lower().replace(' ', '_')}"
    if lineup not in st.session_state:
        st.session_state[lineup] = {"Player": [], "Position": [], "Team": [], "Salary": [], "Points": []}

    # Display lineup as DataFrame in "Add Mode" and as clickable AgGrid in "Delete Mode"
    if toggle_mode_state == "Add Mode":

        lineup_container.dataframe(data=pd.DataFrame(st.session_state[lineup]), hide_index=True, use_container_width=True)
        validate_btn_col, download_btn_col = st.columns(2)
        with validate_btn_col:
            validate_btn_container = st.empty()
        with download_btn_col:
            download_btn_container = st.empty()
    else:

        if len(pd.DataFrame(st.session_state[lineup])) > 0:
            builder = GridOptionsBuilder.from_dataframe(pd.DataFrame(st.session_state[lineup]))
            builder.configure_selection(selection_mode="multiple", use_checkbox=True)
            grid_options = builder.build()
            with lineup_container:
                lineup_deletable = AgGrid(pd.DataFrame(st.session_state[lineup]), columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW, gridOptions=grid_options)
            delete_btn_container = st.empty()

            # Update lineup and salary if user clicks "Delete Players" button
            if delete_btn_container.button(label="🗑️ Delete Players"):
                
                if lineup_deletable["selected_rows"]:
                    selected_players = lineup_deletable["selected_rows"]
                    record_indicies = [selected_player["_selectedRowNodeInfo"]["nodeRowIndex"] for selected_player in selected_players]
                    updated_index = 0
                    for index in record_indicies:
                        for header, column in st.session_state[lineup].items():
                            column.pop(index - updated_index)
                        updated_index += 1
                    update_salary(lineup=pd.DataFrame(st.session_state[lineup]), key_name=salary_cap)
                    salary_cap_container.markdown(body=f"**Remaining Salary**: $" + "{:,}".format(st.session_state[salary_cap]))
                    if len(pd.DataFrame(st.session_state[lineup])) > 0:
                        with lineup_container:
                            lineup_deletable = AgGrid(pd.DataFrame(st.session_state[lineup]), columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW, gridOptions=grid_options)
                    else:
                        lineup_container.markdown(body="No new players to delete.")
                        delete_btn_container.empty()
        else:
            lineup_container.markdown(body="No new players to delete.")

    # Add player to lineup
    st.header(body="Lineup Options", divider="grey")
    st.markdown(body="Click a player to add them to your lineup.")
    tab_options = ["All", "DST", "FLEX", "QB", "RB", "TE", "WR"]
    tab_options_index = 0
    for tab in st.tabs(tab_options):
        if tab_options[tab_options_index] == "All":
            tab_contents = lineup_options
        elif tab_options[tab_options_index] == "FLEX":
            flex_options = lineup_options[lineup_options["Position"] != "DST"].copy()
            flex_options["Position"] = "FLEX"
            tab_contents = flex_options
        else:
            tab_contents = lineup_options[lineup_options["Position"] == tab_options[tab_options_index]].copy()
        tab_options_index += 1
        tab_contents.sort_values(by="Points", ascending=False, inplace=True)
        builder = GridOptionsBuilder.from_dataframe(tab_contents)
        
        # Allow lineup editing in "Add Mode"; disable it in "Delete Mode"
        if toggle_mode_state == "Add Mode":
            builder.configure_selection(selection_mode="single", use_checkbox=False)
        else:
            builder.configure_selection(selection_mode="disabled", use_checkbox=False)
        builder.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
        grid_options = builder.build()
        with tab:
            tabs_contents = AgGrid(tab_contents, custom_css={"#gridToolBar": {"padding-bottom": "0px !important"}}, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW, gridOptions=grid_options)
            if tabs_contents["selected_rows"]:

                # Update lineup and salary with selected player
                selected_player = tabs_contents["selected_rows"][0]
                updated_lineup = update_lineup(selected_player=selected_player, key_name=lineup, msg_container=msg_container)
                update_salary(lineup=updated_lineup, key_name=salary_cap)
                salary_cap_container.markdown(body=f"**Remaining Salary**: $" + "{:,}".format(st.session_state[salary_cap]))
                lineup_container.dataframe(data=updated_lineup, hide_index=True, use_container_width=True)
    
    # Display download and validate buttons if toggle mode state is "Add Mode" and user has selected at least one lineup player
    if toggle_mode_state == "Add Mode" and len(pd.DataFrame(st.session_state[lineup])) > 1:
        csv_file, file_name = download_lineup(lineup=pd.DataFrame(st.session_state[lineup]), nfl_reg_season_week=nfl_reg_season_week)
        download_btn_container.download_button(label="⬇️ Download Lineup", data=csv_file, file_name=file_name, mime="text/csv")
        if validate_btn_container.button(label="✅ Validate Lineup"):
            msg, is_valid = validate_lineup(lineup=pd.DataFrame(st.session_state[lineup]), key_name=salary_cap)
            if is_valid:
                msg_container.success(body=msg, icon="✅")
            else:
                msg_container.error(body=msg, icon="🚨")

        

