-- nfl_dfs_feed
DROP TABLE IF EXISTS nfl_dfs_feed;

CREATE TABLE nfl_dfs_feed(
    record_id SERIAL PRIMARY KEY,
    dataset VARCHAR(255),
    game_id VARCHAR(255),
    date DATE,
    week INT,
    start_time_et TIME,
    player_id VARCHAR(255),
    player_dst VARCHAR(255),
    team VARCHAR(255),
    opponent VARCHAR(255),
    venue VARCHAR(255),
    position_dk VARCHAR(255),
    position_fd VARCHAR(255),
    salary_dk INT,
    salary_fd INT,
    fpts_dk NUMERIC(5,2),
    fpts_fd NUMERIC(5,2)
);

-- upcoming_week_salary
DROP TABLE IF EXISTS upcoming_week_salaries;

CREATE TABLE upcoming_week_salaries(
    record_id SERIAL PRIMARY KEY,
    bd_id VARCHAR(255),
    name VARCHAR(255),
    current_team VARCHAR(255),
    game_info VARCHAR(255),
    upcoming_week_id_dk VARCHAR(255),
    name_dk VARCHAR(255),
    position_dk VARCHAR(255),
    upcoming_week_salary_dk INT,
    upcoming_week_id_fd VARCHAR(255),
    name_fd VARCHAR(255),
    position_fd VARCHAR(255),
    upcoming_week_salary_fd INT,
    upcoming_week_num INT 
);

-- teams table
DROP TABLE IF EXISTS teams;

CREATE TABLE teams(
    record_id SERIAL PRIMARY KEY,
    bd_initial CHAR(3),
    nfl_initial VARCHAR(255),
    team_long_name VARCHAR(255),
    team_short_name VARCHAR(255),
    team_nickname VARCHAR(255),
    division VARCHAR(255),
    conference CHAR(3) 
);

-- nfl_dk_contest_standings
DROP TABLE IF EXISTS nfl_dk_contest_standings;

CREATE TABLE nfl_dk_contest_standings(
    record_id SERIAL PRIMARY KEY,
    entry_name VARCHAR(255),
    points DECIMAL(5,2),
    contest_key BIGINT,
    contest_date_est DATE,
    dst VARCHAR(255),
    flex VARCHAR(255),
    qb VARCHAR(255),
    rb1 VARCHAR(255),
    rb2 VARCHAR(255),
    te VARCHAR(255),
    wr1 VARCHAR(255),
    wr2 VARCHAR(255),
    wr3 VARCHAR(255)
);

-- nba_dk_contest_standings
DROP TABLE IF EXISTS nba_dk_contest_standings;

CREATE TABLE nba_dk_contest_standings(
    record_id SERIAL PRIMARY KEY,
    entry_name VARCHAR(255),
    points DECIMAL(5,2),
    contest_key BIGINT,
    contest_date_est DATE,
    c VARCHAR(255),
    f VARCHAR(255),
    g VARCHAR(255),
    pf VARCHAR(255),
    pg VARCHAR(255),
    sf VARCHAR(255),
    sg VARCHAR(255),
    util VARCHAR(255)
);