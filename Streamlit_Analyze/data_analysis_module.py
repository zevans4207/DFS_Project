# Import libraries
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import streamlit as st
import uuid

# Connect to database
def connect_to_db(username, password, host, port, db_name):
    db_url = f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(db_url)
    try:
        connection = engine.connect()
        return engine, connection
    except Exception as e:
        st.error(body=f"**Error**: {e}", icon="🚨")
        return None, None

# Get weeks in NFL/NBA regular season
def get_reg_season_weeks(sports_league, contest_dates_unique):
    if sports_league == "NFL":
        reg_season_week_1 = datetime.strptime("2023-09-07", "%Y-%m-%d").isocalendar()[1]
    elif sports_league == "NBA":
        reg_season_week_1 = datetime.strptime("2023-10-24", "%Y-%m-%d").isocalendar()[1]
    reg_season_weeks = []
    for date in contest_dates_unique:
        reg_season_week = date.isocalendar()[1] - reg_season_week_1
        if reg_season_week == 0:
            reg_season_week = 1
        reg_season_weeks.append(f"Week {reg_season_week}")
    reg_season_weeks_unique = []
    [reg_season_weeks_unique.append(week) for week in reg_season_weeks if week not in reg_season_weeks_unique]
    if len(reg_season_weeks_unique) > 1:
        reg_season_weeks_unique.append("All Weeks")
    return reg_season_week_1, reg_season_weeks_unique

# Format axis labels in Matplotlib bar graph to include "K" for thousands and "M" for millions
def millions_formatter(axis, pos):
    if axis >= 1e6:
        return f"{axis / 1e6:.1f}M"
    elif axis >= 1e3:
        return f"{axis / 1e3:.0f}K"
    else:
        return str(int(axis))

# Update lineup with selected player
def update_lineup(selected_player, key_name, msg_container):

    # Append selected player to end of lineup if lineup does not exceed 9 player limit
    if len(pd.DataFrame(st.session_state[key_name])) < 9:
        for header, column in st.session_state[key_name].items():
            if header == "Player":
                column.append(selected_player[header])
            elif header == "Position":
                column.append(selected_player[header])
            elif header == "Team":
                column.append(selected_player[header])
            elif header == "Salary":
                column.append(selected_player[header])
            elif header == "Points":
                column.append(selected_player[header])
    else:
        msg_container.error(body=f"**Error**: Unable to add player. Lineup cannot exceed 9 player limit.", icon="🚨")

     # BUG: update_lineup function erroneously adds players from previous tabs in "Lineup Options" section, causing duplicates.
     # Due to Streamlit limitations, rows can't be programmatically deselected in Aggrid. 
     # Workaround: Utilize pandas' drop_duplicates to remove duplicates from the lineup.
    updated_lineup = pd.DataFrame(data=st.session_state[key_name])
    updated_lineup.drop_duplicates(keep="first", inplace=True)
    st.session_state[key_name] = updated_lineup.to_dict(orient="list")
    return updated_lineup

# Update salary cap
def update_salary(lineup, key_name, salary_cap=50000):
    
    st.session_state[key_name] = salary_cap - lineup["Salary"].sum()

# Download lineup as CSV file
def download_lineup(lineup, nfl_reg_season_week):

    csv_file = lineup.to_csv(index=False).encode("utf-8")
    file_name = f"nfl_lineup_{nfl_reg_season_week.lower().replace(' ','_')}.csv"
    return csv_file, file_name

# Validate lineup
def validate_lineup(lineup, key_name, salary_cap=50000):

    # Return error and False for lineups with more than 9 players
    if len(lineup) < 9:
        return f"**Error**: Invalid lineup. Lineup subceeds 9 player limit by {9 - len(lineup) } players.", False

    # Return error and False for duplicate players
    lineup_players = lineup["Player"].value_counts()
    duplicate_players = lineup_players[lineup_players > 1].reset_index()["Player"].tolist()
    if duplicate_players:
        return f"**Error**: Invalid lineup. Duplicate players detected for {duplicate_players}.", False
    
    # Return error and False for invalud number of player positions
    # Requirements are 2 players for RB, 3 for WR, and 1 for everything else
    lineup_positions = lineup["Position"].value_counts().reset_index()
    expected_position_counts = { "DST": 1, "FLEX": 1, "QB": 1, "RB": 2, "TE": 1, "WR": 3 }
    actual_position_counts = {}
    for index, row in lineup_positions.iterrows():
        actual_position_counts[row["Position"]] = row["count"]
    actual_position_counts_sorted = dict(sorted(actual_position_counts.items(), key=lambda item: item[0]))
    if expected_position_counts != actual_position_counts:
        return f"**Error**: Invalid number of positions. Lineup must be 2 RBs, 3 WRs, and 1 of everything else.", False

    # Return error and False for exceeding salary cap
    if st.session_state[key_name] < 0:
        return "**Error**: Invalid lineup. $" + "{:,}".format(salary_cap) + " salary cap exceeded by $" + "{:,}".format(abs(st.session_state[key_name])) + ".", False

    # Return success and True if lineup passes previous validations
    return "**Success**: This lineup is valid!", True





        
