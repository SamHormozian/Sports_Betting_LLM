import pandas as pd
import os

# Directories for the raw data and processed data
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")

# Ensure processed directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Standardize team names (NBA and NFL teams)
NBA_TEAM_MAPPING = {
    "ATL Hawks": "Atlanta Hawks", "BOS Celtics": "Boston Celtics", "BKN Nets": "Brooklyn Nets",
    "CHA Hornets": "Charlotte Hornets", "CHI Bulls": "Chicago Bulls", "CLE Cavaliers": "Cleveland Cavaliers",
    "DAL Mavericks": "Dallas Mavericks", "DEN Nuggets": "Denver Nuggets", "DET Pistons": "Detroit Pistons",
    "GS Warriors": "Golden State Warriors", "HOU Rockets": "Houston Rockets", "IND Pacers": "Indiana Pacers",
    "LA Clippers": "Los Angeles Clippers", "LA Lakers": "Los Angeles Lakers", "MEM Grizzlies": "Memphis Grizzlies",
    "MIA Heat": "Miami Heat", "MIL Bucks": "Milwaukee Bucks", "MIN Timberwolves": "Minnesota Timberwolves",
    "NO Pelicans": "New Orleans Pelicans", "NY Knicks": "New York Knicks", "OKC Thunder": "Oklahoma City Thunder",
    "ORL Magic": "Orlando Magic", "PHI 76ers": "Philadelphia 76ers", "PHX Suns": "Phoenix Suns",
    "POR Trail Blazers": "Portland Trail Blazers", "SAC Kings": "Sacramento Kings", "SA Spurs": "San Antonio Spurs",
    "TOR Raptors": "Toronto Raptors", "UTA Jazz": "Utah Jazz", "WAS Wizards": "Washington Wizards"
}

NFL_TEAM_MAPPING = {
    "ARI Cardinals": "Arizona Cardinals", "ATL Falcons": "Atlanta Falcons", "BAL Ravens": "Baltimore Ravens",
    "BUF Bills": "Buffalo Bills", "CAR Panthers": "Carolina Panthers", "CHI Bears": "Chicago Bears",
    "CIN Bengals": "Cincinnati Bengals", "CLE Browns": "Cleveland Browns", "DAL Cowboys": "Dallas Cowboys",
    "DEN Broncos": "Denver Broncos", "DET Lions": "Detroit Lions", "GB Packers": "Green Bay Packers",
    "HOU Texans": "Houston Texans", "IND Colts": "Indianapolis Colts", "JAX Jaguars": "Jacksonville Jaguars",
    "KC Chiefs": "Kansas City Chiefs", "LA Chargers": "Los Angeles Chargers", "LA Rams": "Los Angeles Rams",
    "LV Raiders": "Las Vegas Raiders", "MIA Dolphins": "Miami Dolphins", "MIN Vikings": "Minnesota Vikings",
    "NE Patriots": "New England Patriots", "NO Saints": "New Orleans Saints", "NY Giants": "New York Giants",
    "NY Jets": "New York Jets", "PHI Eagles": "Philadelphia Eagles", "PIT Steelers": "Pittsburgh Steelers",
    "SF 49ers": "San Francisco 49ers", "SEA Seahawks": "Seattle Seahawks", "TB Buccaneers": "Tampa Bay Buccaneers",
    "TEN Titans": "Tennessee Titans", "WAS Commanders": "Washington Commanders"
}

# Combine CSV files
def combine_csv_files(file_prefix, output_file, required_columns=None):
    all_files = [f for f in os.listdir(RAW_DIR) if f.startswith(file_prefix) and f.endswith(".csv")]
    combined_dfs = []
    for file in all_files:
        df = pd.read_csv(os.path.join(RAW_DIR, file))
        df.columns = df.columns.str.strip().str.lower()

        # Standardize date column
        for possible_date_col in ["date", "game_date", "commence_time", "match_date"]:
            if possible_date_col in df.columns:
                df = df.rename(columns={possible_date_col: "date"})
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                break
        else:
            print(f"Warning: No date column found in {file}. Adding placeholder date.")
            df["date"] = pd.Timestamp.now().date()  # Placeholder date

        # Filter required columns
        if required_columns:
            df = df[[col for col in required_columns if col in df.columns]]
        combined_dfs.append(df)

    if combined_dfs:
        combined_df = pd.concat(combined_dfs, ignore_index=True)
        combined_df.to_csv(os.path.join(PROCESSED_DIR, output_file), index=False)
        print(f"Combined data saved to {output_file}")
    else:
        print(f"No files found for prefix '{file_prefix}'.")

# Remove duplicates
def remove_duplicates(input_file, output_file):
    df = pd.read_csv(os.path.join(PROCESSED_DIR, input_file))
    df = df.drop_duplicates()
    df.to_csv(os.path.join(PROCESSED_DIR, output_file), index=False)
    print(f"Duplicates removed, saved to {output_file}")

# Clean injury data
def clean_injury_data(input_file, output_file, sport):
    """
    Clean injury data: standardize team names and handle missing values.
    """
    print(f"Cleaning injury data for {sport.upper()}...")
    df = pd.read_csv(os.path.join(RAW_DIR, input_file))

    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # Standardize date column
    for possible_date_col in ["date", "game_date", "match_date"]:
        if possible_date_col in df.columns:
            df = df.rename(columns={possible_date_col: "date"})
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            break
    else:
        df["date"] = pd.Timestamp.now().date()
        print(f"Warning: No date column found in {input_file}. Added placeholder date.")

    # Standardize team names
    team_mapping = NBA_TEAM_MAPPING if sport == "nba" else NFL_TEAM_MAPPING
    if "team" in df.columns:
        df["team"] = df["team"].replace(team_mapping)

    # Fill missing values
    df = df.fillna("unknown")

    # Save the cleaned injury data
    df.to_csv(os.path.join(PROCESSED_DIR, output_file), index=False)
    print(f"Injury data cleaned and saved to {output_file}")

if __name__ == "__main__":
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # NBA Player Stats
    combine_csv_files("nba_stats", "nba_stats_combined.csv", required_columns=[
        "date", "player", "team", "pos", "g", "pts", "ast", "trb", "fg%", "3p%", "ft%"
    ])
    remove_duplicates("nba_stats_combined.csv", "nba_stats_final.csv")

    # NFL Player Stats
    combine_csv_files("nfl_stats", "nfl_stats_combined.csv", required_columns=[
        "date", "player", "team", "pos", "g", "yds", "td", "int", "rate"
    ])
    remove_duplicates("nfl_stats_combined.csv", "nfl_stats_final.csv")

    # NFL Betting Odds
    combine_csv_files("americanfootball_nfl_odds", "nfl_odds_combined.csv", required_columns=[
        "date", "home_team", "away_team", "bookmaker", "market_type", "price", "point"
    ])
    remove_duplicates("nfl_odds_combined.csv", "nfl_odds_cleaned.csv")

    # NBA Betting Odds
    combine_csv_files("basketball_nba_odds", "nba_odds_combined.csv", required_columns=[
        "date", "home_team", "away_team", "bookmaker", "market_type", "price", "point"
    ])
    remove_duplicates("nba_odds_combined.csv", "nba_odds_cleaned.csv")

    # NBA Injury Data
    clean_injury_data("nba_injuries.csv", "nba_injuries_cleaned.csv", sport="nba")

    # NFL Injury Data
    clean_injury_data("nfl_injuries.csv", "nfl_injuries_cleaned.csv", sport="nfl")

    print("Data cleaning and preprocessing complete!")
