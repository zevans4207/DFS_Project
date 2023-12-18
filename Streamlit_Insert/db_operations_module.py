# Import libraries
import re
import boto3
from datetime import datetime
import openpyxl
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import text

# Load environment variables from .env
load_dotenv(os.path.join(os.getcwd(), "config", ".env"))
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Function to retrieve database connection details
def get_connection_details(conn):
    return {
        "Database Name": conn.get_dsn_parameters()["dbname"],
        "Database Username": conn.get_dsn_parameters()["user"],
        "Database Host": conn.get_dsn_parameters()["host"],
        "Database Port": conn.get_dsn_parameters()["port"]
    }

# Function to retrieve table names in the database
def list_tables(cursor):
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cursor.fetchall()
    return { "Table Name": [table[0] for table in tables] }

# Function to get record count of a table
def get_record_count(cursor, table_name):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    return { "Record Count": cursor.fetchone()[0] }

# Function to get the first or last 10 records from a table
def get_records(table_name, engine_connection, limit=10, first=True):
    order = "ASC" if first else "DESC"
    sql = text(f"""
        SELECT * FROM {table_name} 
        ORDER BY record_id {order} 
        LIMIT {limit};
    """)
    if first:
        return pd.read_sql(sql, engine_connection)
    else:
        return pd.read_sql(sql, engine_connection).iloc[::-1]

# Function to drop and re-create all tables in database
def delete_records(conn, cursor):
    sql_scripts = [os.path.join("../SQL_Scripts/", file_name) for file_name in os.listdir("../SQL_Scripts/")]
    for script in sql_scripts:
        sql_file = open(script, "r")
        lines = sql_file.readlines()
        # Remove comments from SQL script
        lines = [line for line in lines if not line.startswith("--")]
        # Remove newline characters
        lines = [line.replace("\n","") for line in lines]
        lines = [line for line in lines if line != ""]
        # Join list elements into string and remove extraneous spacing
        lines = " ".join(lines)
        lines = re.sub(r"\s+"," ", lines)
        lines = re.sub(r"(;|\(|\))\s+", r"\1", lines)
        # Split string at ";" to get the individual SQL commands
        sql_commands = lines.split(";")
        # Remove empty strings from list of SQl commands
        sql_commands = [command for command in sql_commands if command != ""]
        # Run each SQL command
        for command in sql_commands:
            cursor.execute(f"{command};")
            conn.commit()
    
# Function to get files in S3 bucket
def get_s3_files():
    #Create S3 client and get files listed in "zachs-data" S3 bucket
    s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3_bucket_files = s3.list_objects_v2(Bucket="zachs-data")
    # Get "nfl-season-dfs-feed" (Player Stats) files in "NFL/" folder of S3 bucket
    nfl_season_dfs_feed_files = []
    for file in s3_bucket_files["Contents"]:
        if file["Key"].startswith("NFL/"):
            if "nfl-season-dfs-feed" in file["Key"]:
                file_name = file["Key"].replace("NFL/","")
                file_date = file_name.split("-")[:3]
                file_date = "-".join(file_date)
                file_date = datetime.strptime(file_date,"%m-%d-%Y")
                nfl_season_dfs_feed_files.append((file_date, file_name))
    # Sort "nfl-season-dfs-feed" (Player Stats) files by date
    nfl_season_dfs_feed_files = [item[1] for item in sorted(nfl_season_dfs_feed_files,key=lambda x: x[0],reverse=True)]
    # Get "contest-standings" (Contest Standings) files in "NFL/" folder of S3 bucket
    contest_standings_files = []
    for file in s3_bucket_files["Contents"]:
        if file["Key"].startswith("NFL/"):
            if "contest-standings" in file["Key"]:
                file_date = file["LastModified"]
                file_name = file["Key"].replace("NFL/","")
                contest_standings_files.append((file_date, file_name))
    # Sort "contest-standings" (Contest Standings) files by date
    contest_standings_files = [item[1] for item in sorted(contest_standings_files,key=lambda x: x[0],reverse=True)]
    # Get most recent "draftkings-contest-entry-history" (Draftkings History) file in "NFL/" folder of S3 bucket
    most_recent_draftkings_contest_entry_history_file = None
    most_recent_date = None
    for file in s3_bucket_files["Contents"]:
        if file["Key"].startswith("NFL/"):
            if "draftkings-contest-entry-history" in file["Key"]:
                if most_recent_date is None or most_recent_date < file["LastModified"]:
                    most_recent_date = file["LastModified"]
                    file_name = file["Key"].replace("NFL/","")
                    most_recent_draftkings_contest_entry_history_file = file_name
    # Append most recent "draftkings-contest-entry-history" (Draftkings History) file to list of "contest-standings" (Contest Standings) files
    contest_standings_and_draftkings_contest_entry_history_files = contest_standings_files + [most_recent_draftkings_contest_entry_history_file]
    # Organize the files by the database tables they go into
    return {
        "nfl_season_dfs_feed_tables": nfl_season_dfs_feed_files,
        "dk_contest_standings_tables": contest_standings_and_draftkings_contest_entry_history_files
    }


# Function to clean and insert data into database
    # Parameters:
        # table_name: The name of the database table where data will be inserted
        # group_name: The name of the group of files where data will be cleaned/extracted for database insertion
        # file_group: A list of file names in the group
        # conn: Establishes a database connection for data insertion
def clean_insert_data(table_name, group_name, file_group, engine):
    s3 = boto3.client("s3", endpoint_url ="http://s3.us-east-1.wasabisys.com", aws_access_key_id="3MWEIPB9THV1GUFMS3W0", aws_secret_access_key="gBjfvvL7NsOj3i50DeqlhPB0snCe2SJhRD2azLuM")
    df = None
    nfl_dfs_feed_insert_counter = 0
    teams_insert_counter = 0
    most_recent_draftkings_contest_entry_history_file = None
    files_to_clean = []
    # Download ALL files in file group in reversed order (oldest to newest)
    if group_name == "nfl_season_dfs_feed_tables":
        for file in reversed(file_group):
            s3.download_file("zachs-data",f"NFL/{file}",f"../Raw_Data/{file}")
            files_to_clean.append(file)
    elif group_name == "dk_contest_standings_tables":
        most_recent_draftkings_contest_entry_history_file = file_group[-1]
        s3.download_file("zachs-data",f"NFL/{most_recent_draftkings_contest_entry_history_file}",f"../Raw_Data/{most_recent_draftkings_contest_entry_history_file}")
        for file in reversed(file_group[:-1]):
            s3.download_file("zachs-data",f"NFL/{file}",f"../Raw_Data/{file}")
            files_to_clean.append(file)
    for file in files_to_clean:
        # If file group is "nfl_season_dfs_feed_tables", go through the sheets in each file (excluding "METADATA" sheet) and clean them
        if group_name == "nfl_season_dfs_feed_tables":
            sheets = openpyxl.load_workbook(f"../Raw_Data/{file}").sheetnames
            # Remove "METADATA" sheet
            if "METADATA" in sheets:
                del sheets[sheets.index("METADATA")]
            # If table name is "nfl_dfs_feed", load the "NFL-DFS-FEED" sheet into DataFrame and clean it
            if table_name == "nfl_dfs_feed":
                sheet = sheets[sheets.index("NFL-DFS-FEED")]
                df = pd.read_excel(f"../Raw_Data/{file}", sheet_name=sheet, header=1)
                # Rename DataFrame columns
                df.rename(columns={
                    'BIGDATABALL\nDATASET': 'dataset',
                    'GAME-ID': 'game_id',
                    'DATE': 'date',
                    'WEEK#': 'week',
                    'START\nTIME\n(ET)': 'start_time_et',
                    'PLAYER-ID': 'player_id',
                    'PLAYER / DST': 'player_dst',
                    'TEAM': 'team',
                    'OPPONENT': 'opponent',
                    'VENUE \n(R/H)': 'venue',
                    'DRAFTKINGS': 'position_dk',
                    'FANDUEL': 'position_fd',
                    'for DRAFTKINGS\n"Classic" Contests': 'salary_dk',
                    'for FANDUEL\n"Full Roster" Contests': 'salary_fd',
                    'DRAFTKINGS.1': 'fpts_dk',
                    'FANDUEL.1': 'fpts_fd'
                },inplace=True)
                #Drop extraneous first row from DataFrame
                df.drop(labels=0,axis=0,inplace=True)
                #Format start_time_et column
                df["start_time_et"] = pd.to_datetime(df["start_time_et"],format="%I:%M %p").dt.time
                # If table is NOT empty (i.e., nfl_dfs_feed_insert_counter > 0), only get DataFrame rows where "date" column is
                # Greater than the datestamp on previous file (ex: 10-09-2023) and less than or equal to the datestamp on the
                # Most recent file (ex: 10-16-2023)
                if nfl_dfs_feed_insert_counter > 0:
                    most_recent_file_date = file.split("-")[0:3]
                    most_recent_file_date = "-".join(most_recent_file_date)
                    most_recent_file_date = datetime.strptime(most_recent_file_date,"%m-%d-%Y")
                    penultimate_file_date = file_group[file_group.index(file) + 1].split("-")[0:3]
                    penultimate_file_date = "-".join(penultimate_file_date)
                    penultimate_file_date = datetime.strptime(penultimate_file_date,"%m-%d-%Y")
                    df = df[(df["date"] > penultimate_file_date) & (df["date"] <= most_recent_file_date)]
                
                nfl_dfs_feed_insert_counter += 1

                # Load cleaned DataFrame into SQL table
                df.to_sql(table_name, engine, if_exists="append", index=False)

            # If table name is "upcoming_week_salaries" and "WEEK-# SALARIES" is a sheet in file
            # Load the "WEEK-# SALARIES" sheet into DataFrame and clean it
            elif table_name == "upcoming_week_salaries" and "WEEK-" in " ".join(sheets) and "SALARIES" in " ".join(sheets):
                sheet = [sheet for sheet in sheets if sheet != "NFL-DFS-FEED" and sheet != "TEAMS"][0]
                df = pd.read_excel(f"../Raw_Data/{file}", sheet_name=sheet, header=0)
                # Rename DataFrame columns
                df.rename(columns={
                    'NOTICE: Salaries from upcoming week will be up until Thursday!': 'bd_id',
                    'Unnamed: 1': 'name',
                    'Unnamed: 2': 'current_team',
                    'Unnamed: 3': 'game_info',
                    'DRAFTKINGS': 'upcoming_week_id_dk',
                    'Unnamed: 5': 'name_dk',
                    'Unnamed: 6': 'position_dk',
                    'Unnamed: 7': 'upcoming_week_salary_dk',
                    'FANDUEL': 'upcoming_week_id_fd',
                    'Unnamed: 9': 'name_fd',
                    'Unnamed: 10': 'position_fd',
                    'Unnamed: 11': 'upcoming_week_salary_fd' 
                },inplace=True)
                #Get upcoming week number and add it as a column to the DataFrame
                df["upcoming_week_num"] = df.iloc[0,4].split(" ")[-1].split("-")[-1]
                #Drop extraneous first row
                df.drop(labels=0,axis=0,inplace=True)
                # Load cleaned DataFrame into SQL table
                df.to_sql(table_name, engine, if_exists="append", index=False)

            # If table name is "teams", load the "TEAMS" sheet into DataFrame and clean it
            # "teams" table only needs to have data cleaned/inserted once, so increment teams_insert_counter
            # To signify when "teams" table has been filled
            elif table_name == "teams" and teams_insert_counter < 1:
                sheet = sheets[sheets.index("TEAMS")]
                # Load file sheet into DataFrame
                df = pd.read_excel(f"../Raw_Data/{file}", sheet_name=sheet, header=0)
                # Rename DataFrame columns
                df.rename(columns={
                    'BIGDATABALL\nINITIAL': 'bd_initial',
                    'NFL.com\nINITIAL': 'nfl_initial',
                    'TEAM\nLONG NAME': 'team_long_name',
                    'TEAM\nSHORT NAME': 'team_short_name',
                    'TEAM\nNICK NAME': 'team_nickname',
                    'DIVISION': 'division',
                    'CONFERENCE': 'conference'
                },inplace=True)
                # Increment teams_insert_counter
                teams_insert_counter += 1
                # Load cleaned DataFrame into SQL table
                df.to_sql(table_name, engine, if_exists="append", index=False)
        # If file group is "dk_contest_standings_tables", go through the Contest Standings and Draftkings History files
        # And clean and merge them
        elif group_name == "dk_contest_standings_tables":
            # Load files into DataFrames and create contest_key
            cs_df = pd.read_csv(f"../Raw_Data/{file}",dtype={"Player":str,"Roster Position":str,"%Drafted":str})
            dk_df = pd.read_csv(f"../Raw_Data/{most_recent_draftkings_contest_entry_history_file}")
            cs_contest_key = int(file.split("-")[-1].replace(".csv",""))
            # Drop extraneous "Unnamed: 6" column from cs_df
            cs_df.drop(labels="Unnamed: 6",axis=1,inplace=True)
            #Create "Contest_Key" column in cs_df and set its values to cs_contest_key
            cs_df["Contest_Key"] = cs_contest_key
            # Filter dk_df where "Sport" equals "NFL" or "NBA", "Game_Type" equals "Classic", and "Contest_Key" equals cs_contest_key
            if table_name == "nfl_dk_contest_standings":
                dk_df = dk_df[(dk_df["Sport"] == "NFL") & (dk_df["Game_Type"] == "Classic") & (dk_df["Contest_Key"] == cs_contest_key)]
            elif table_name == "nba_dk_contest_standings":
                dk_df = dk_df[(dk_df["Sport"] == "NBA") & (dk_df["Game_Type"] == "Classic") & (dk_df["Contest_Key"] == cs_contest_key)]
            # Drop all columns from dk_df excluding "Contest_Key" and "Contest_Date_EST"
            dk_df.drop(labels=[
                "Sport","Game_Type","Entry_Key",
                "Entry","Place","Points",
                "Winnings_Non_Ticket","Winnings_Ticket","Contest_Entries",
                "Entry_Fee","Prize_Pool","Places_Paid"
            ],axis=1,inplace=True)
            # Create new DataFrame merging cs_df and dk_df on "Contest_Key" column
            cs_dk_df = cs_df.merge(dk_df,on="Contest_Key",how="inner")
            # If merged DataFrame is not empty clean it and insert into database
            if cs_dk_df.empty == False:
                # Drop duplicate values from cs_dk_df
                cs_dk_df.drop_duplicates(inplace=True,ignore_index=True)
                # Drop rows from cs_dk_dk where "Lineup" column equals NaN
                cs_dk_df.dropna(subset="Lineup",inplace=True)
                # Drop extraneous columns from cs_dk_df
                cs_dk_df.drop(labels=["Rank","EntryId","TimeRemaining","Player","Roster Position","%Drafted","FPTS"],axis=1,inplace=True)
                # Create 9 new position columns for cs_dk_df based on sport type
                if table_name == "nfl_dk_contest_standings":
                    positions = ["DST", "FLEX", "QB", "RB1", "RB2", "TE", "WR1", "WR2", "WR3"]
                elif table_name == "nba_dk_contest_standings":
                    positions = ["C", "F", "G", "PF", "PG", "SF", "SG", "UTIL"]
                for position in positions:
                    cs_dk_df[position] = ""
                # Populate position columns with player names
                for row_index, row in cs_dk_df.iterrows():
                    lineup = re.sub(r"\s+", " ", row["Lineup"])
                    if table_name == "nfl_dk_contest_standings":
                        lineup = lineup.replace(" RB "," RB1 ",1)
                        lineup = lineup.replace(" RB "," RB2 ",1)
                        lineup = lineup.replace(" WR "," WR1 ",1)
                        lineup = lineup.replace(" WR "," WR2 ",1)
                        lineup = lineup.replace(" WR "," WR3 ",1)
                    lineup = lineup.split(" ")
                    pos_index = 0
                    player_name = None
                    # Get the player name associated with each position
                    for position in positions:
                        if pos_index+1 < len(positions):
                            player_name = lineup[lineup.index(position)+1:lineup.index(positions[pos_index+1])]
                            pos_index+=1
                        else:
                            player_name = lineup[lineup.index(position)+1:]
                        if len(player_name) > 1:
                            player_name = " ".join(player_name)
                        else:
                            player_name = player_name[0]
                        #Assign player name to the appropriate position column in the current row
                        cs_dk_df.at[row_index, position] = player_name
                # Drop "Lineup" column from cs_dk_df
                cs_dk_df.drop(labels="Lineup",axis=1,inplace=True)
                # Strip "(#/#)" pattern from "EntryName" column (ex: remove "(5/7)" from  "tdotmcghee (5/7)"")
                # And remove any trailing spaces on the right
                cs_dk_df["EntryName"] = cs_dk_df["EntryName"].str.replace(r"\(\d+/\d+\)","", regex=True).str.rstrip()
                # Rename cs_dk_df columns
                if table_name == "nfl_dk_contest_standings":
                    cs_dk_df.rename(columns={
                        "Rank":"rank","EntryId":"entry_id","EntryName":"entry_name",
                        "TimeRemaining":"time_remaining","Points":"points","Player":"player",
                        "Roster Position":"roster_position","%Drafted":"drafted_pct","FPTS":"fpts",
                        "Contest_Key":"contest_key","Contest_Date_EST":"contest_date_est","DST":"dst",
                        "FLEX":"flex","QB":"qb","RB1":"rb1",
                        "RB2":"rb2","TE":"te","WR1":"wr1",
                        "WR2":"wr2","WR3":"wr3"
                    },inplace=True)
                elif table_name == "nba_dk_contest_standings":
                    cs_dk_df.rename(columns={
                        "Rank":"rank","EntryId":"entry_id","EntryName":"entry_name",
                        "TimeRemaining":"time_remaining","Points":"points","Player":"player",
                        "Roster Position":"roster_position","%Drafted":"drafted_pct","FPTS":"fpts",
                        "Contest_Key":"contest_key","Contest_Date_EST":"contest_date_est",
                        "C":"c","F":"f","G":"g","PF":"pf","PG":"pg","SF":"sf","SG":"sg","UTIL":"util"
                    },inplace=True)
                # Load cleaned DataFrame into SQL table
                cs_dk_df.to_sql(table_name, engine, if_exists="append", index=False)

# Function to convert seconds to minutes and hours
def convert_seconds(timer):
    if timer == 1:
        return f"{timer} second"
    elif timer < 60:
        return f"{timer} seconds"
    elif timer == 60:
        return f"{1} minute and {0} seconds"
    elif timer > 60 and timer < 3600:
        return f"{timer // 60} minutes and {timer % 60} seconds"
    elif timer == 3600:
        return f"{1} hour, {0} minutes, and {0} seconds"
    else:
        return f"{timer // 3600} hours, {(timer % 3600) // 60} minutes, and {timer % 60} seconds"

