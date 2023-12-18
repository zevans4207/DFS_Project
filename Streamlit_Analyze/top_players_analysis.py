# Import libraries
import streamlit as st, pandas as pd, matplotlib.pyplot as plt, os, sys
from datetime import datetime
from math import ceil
from matplotlib.ticker import FuncFormatter
from dotenv import load_dotenv
from data_analysis_module import *

# Database connection
if "engine" and "connection" not in st.session_state:
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), "config", ".env"))
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
    
    # Set the page title and overview
    st.markdown("# DraftKings Top N% Analyzer")
    st.header(body="Overview", divider="grey")
    st.markdown("""
        Analyze the specified type and percentage of records from the 2023 DraftKings Contest Standings. To begin, select a sports league in the left sidebar.
    """)

    # Create a section for selecting a sports league
    st.sidebar.header("Select a Sports League")
    sports_leagues = ["NFL", "NBA"]
    sports_league = st.sidebar.selectbox(label="Select a Sports League", options=sports_leagues, index=None, label_visibility="collapsed")

    if sports_league:
        # Retrieve sports league records from database and load into DataFrame
        sql = text(f"""
            SELECT *
            FROM {sports_league.lower()}_dk_contest_standings;
        """)
        df = pd.read_sql(sql, connection)

        # Create a section for selecting a week in the NFL/NBA regular season
        st.sidebar.header("Select a Week")
        reg_season_week_1, reg_season_weeks = get_reg_season_weeks(sports_league=sports_league, contest_dates_unique=pd.unique(df["contest_date_est"].sort_values(ascending=True)))
        reg_season_week = st.sidebar.selectbox(label="Select Week", options=reg_season_weeks, index=None, label_visibility="collapsed")

        if reg_season_week:
            # If user selects a specific week, filter DataFrame to include records from that regular season week
            if reg_season_week != "All Weeks":
                week_num_in_year = [int(word) for word in reg_season_week.split(" ") if word.isdigit()][0] + reg_season_week_1
                df = df[pd.to_datetime(df["contest_date_est"]).dt.isocalendar().week == week_num_in_year]
            
            # Create a section for selecting a percentage of records to extract from the DataFrame
            st.sidebar.header("Select a Percentage")
            percentage = st.sidebar.slider(label="Select a Percentage", min_value=0, max_value=100, step=10, value=0, label_visibility="collapsed")

            if percentage:
                # Get top percentage of records from DataFrame
                df.sort_values(by="points",ascending=False,inplace=True)
                top_n_pct_record_count = ceil(len(df) * (percentage / 100))
                top_n_pct_record_count_delimited = f"{top_n_pct_record_count:,}"
                top_n_pct_records = df.iloc[:top_n_pct_record_count].reset_index(drop=True)

                # Create a section for displaying the first 10 records from the extracted percentage
                if percentage < 100 and reg_season_week == "All Weeks":
                    top_pct_section_title = f"{sports_league} DraftKings Top {percentage}% (All Weeks)"
                    section_overview = f"""
                        Extracted the top {percentage}% of all {sports_league} records from the Draftkings Contest Standings ({top_n_pct_record_count_delimited} extracted records in total). 
                        See first 10 records below. To download full data extraction, click the download button below the table.
                    """
                    csv_file_name = f"top_{percentage}_pct_{sports_league.lower()}_dk_contest_standings_all_weeks.csv"
                elif percentage == 100 and reg_season_week != "All Weeks":
                    top_pct_section_title = f"{sports_league} DraftKings ({reg_season_week})"
                    section_overview = f"""
                        Extracted all {reg_season_week} {sports_league} records from the Draftkings Contest Standings ({top_n_pct_record_count_delimited} extracted records in total). 
                        See first 10 records below. To download full data extraction, click the download button below the table.
                    """
                    csv_file_name = f"{sports_league.lower()}_dk_contest_standings_{reg_season_week.lower().replace(' ','_')}.csv"
                elif percentage == 100 and reg_season_week == "All Weeks":
                    top_pct_section_title = f"{sports_league} DraftKings (All Weeks)"
                    section_overview = f"""
                        Extracted all {sports_league} records from the Draftkings Contest Standings ({top_n_pct_record_count_delimited} extracted records in total). 
                        See first 10 records below. To download full data extraction, click the download button below the table.
                    """
                    csv_file_name = f"{sports_league.lower()}_dk_contest_standings_all_weeks.csv"
                elif percentage != 100 and reg_season_week != "All Weeks":
                    top_pct_section_title = f"{sports_league} DraftKings Top {percentage}% ({reg_season_week})"
                    section_overview = f"""
                        Extracted the top {percentage}% of {reg_season_week} {sports_league} records from the Draftkings Contest Standings ({top_n_pct_record_count_delimited} extracted records in total). 
                        See first 10 records below. To download full data extraction, click the download button below the table.
                    """
                    csv_file_name = f"top_{percentage}_pct_{sports_league.lower()}_dk_contest_standings_{reg_season_week.lower().replace(' ','_')}.csv"
                st.header(body=top_pct_section_title, divider="grey")
                st.markdown(section_overview)
                st.dataframe(data=top_n_pct_records.head(10), hide_index=True, use_container_width=True)
                
                # Create a download button for record extraction
                st.warning(body="**WARNING:** Downloading bigger datasets may result in longer load times and some data loss.", icon="⚠️")
                csv_file = top_n_pct_records.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download All Extracted Records",
                    data=csv_file,
                    file_name=csv_file_name,
                    mime="text/csv",
                    use_container_width=True
                )
                # Create a section for selecting a playing position
                st.sidebar.header("Select a Position")
                if sports_league == "NFL":
                    positions = ["DST", "FLEX", "QB", "RB", "TE", "WR"]
                elif sports_league == "NBA":
                    positions = ["C", "F", "G", "PF", "PG", "SG", "UTIL"]
                position = st.sidebar.selectbox(label="Select a Position", options=positions, index=None, label_visibility="collapsed")
                
                if position:
                    # Melt DataFrame to create "position" and "player" columns
                    if sports_league == "NFL":
                        top_n_pct_records_melted = pd.melt(
                            frame=top_n_pct_records,
                            id_vars=["record_id","entry_name","points","contest_key","contest_date_est"],
                            value_vars=["dst","flex","qb","rb1","rb2","te","wr1","wr2","wr3"],
                            var_name="position",
                            value_name="player"
                        )
                    elif sports_league == "NBA":
                        top_n_pct_records_melted = pd.melt(
                            frame=top_n_pct_records,
                            id_vars=["record_id","entry_name","points","contest_key","contest_date_est"],
                            value_vars=[position.lower() for position in positions],
                            var_name="position",
                            value_name="player"
                    )

                    # Create DataFrame calculating total number of points for each player within each position
                    player_points_by_all_positions = top_n_pct_records_melted.groupby(["position","player"])["points"].sum().reset_index()
                    
                    # Filter DataFrame by selected position
                    if sports_league == "NFL" and position == "RB" or position == "WR":
                        if position == "RB":
                            player_points_by_selected_position = player_points_by_all_positions[(player_points_by_all_positions["position"] == "rb1") | (player_points_by_all_positions["position"] == "rb2")].sort_values(by="points", ascending=False).reset_index(drop=True)
                        elif position == "WR":
                            player_points_by_selected_position = player_points_by_all_positions[(player_points_by_all_positions["position"] == "wr1") | (player_points_by_all_positions["position"] == "wr2") | (player_points_by_all_positions["position"] == "wr3")].sort_values(by="points", ascending=False).reset_index(drop=True)
                    else:
                        player_points_by_selected_position = player_points_by_all_positions[player_points_by_all_positions["position"] == position.lower()].sort_values(by="points", ascending=False).reset_index(drop=True)
                    
                    # Get top 10 records from filtered DataFrame
                    top_10_player_points_by_selected_position = player_points_by_selected_position[:10].sort_values(by="points", ascending=True)
                    
                    # Create a section for displaying the top-performing players within the record extraction
                    if percentage < 100 and reg_season_week == "All Weeks":
                        top_players_section_title = f"Top 10 {position}s in {sports_league} DraftKings Top {percentage}% (All Weeks)"
                    elif percentage == 100 and reg_season_week != "All Weeks":
                        top_players_section_title = f"Top 10 {position}s in {sports_league} DraftKings ({reg_season_week})"
                    elif percentage == 100 and reg_season_week == "All Weeks":
                        top_players_section_title = f"Top 10 {position}s in {sports_league} DraftKings (All Weeks)"
                    elif percentage != 100 and reg_season_week != "All Weeks":
                        top_players_section_title = f"Top 10 {position}s in {sports_league} DraftKings Top {percentage}% ({reg_season_week})"
                    st.header(body=top_players_section_title, divider="grey")

                    # Display filtered DataFrame as bar graph or table, bar graph by default
                    bar_graph_tab, table_tab = st.tabs(["Bar Graph", "Table"])
                    with bar_graph_tab:
                        fig, ax = plt.subplots()
                        ax.barh(top_10_player_points_by_selected_position["player"], top_10_player_points_by_selected_position["points"])
                        ax.set_title(top_players_section_title)
                        ax.xaxis.set_major_formatter(FuncFormatter(millions_formatter))
                        ax.set_xlabel("Points")
                        if position == "DST":
                            ax.set_ylabel("Teams")
                        else:
                            ax.set_ylabel("Players")
                        st.pyplot(fig)
                    with table_tab:
                        top_10_player_points_by_selected_position_table = pd.DataFrame({"Players": top_10_player_points_by_selected_position["player"], "Points": top_10_player_points_by_selected_position["points"]})[::-1]
                        st.dataframe(
                            data=top_10_player_points_by_selected_position_table, 
                            use_container_width=True, 
                            hide_index=True
                        )


