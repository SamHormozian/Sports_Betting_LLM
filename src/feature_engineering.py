import pandas as pd
import os

# Directories
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
FEATURES_DIR = os.path.join(PROJECT_ROOT, "data", "features")

# Ensure features directory exists
os.makedirs(FEATURES_DIR, exist_ok=True)

def load_data():
    """Load cleaned datasets."""
    nba_stats = pd.read_csv(os.path.join(PROCESSED_DIR, "nba_stats_final.csv"))
    nfl_stats = pd.read_csv(os.path.join(PROCESSED_DIR, "nfl_stats_final.csv"))
    nba_odds = pd.read_csv(os.path.join(PROCESSED_DIR, "nba_odds_cleaned.csv"))
    nfl_odds = pd.read_csv(os.path.join(PROCESSED_DIR, "nfl_odds_cleaned.csv"))
    nba_injuries = pd.read_csv(os.path.join(PROCESSED_DIR, "nba_injuries_cleaned.csv"))
    nfl_injuries = pd.read_csv(os.path.join(PROCESSED_DIR, "nfl_injuries_cleaned.csv"))
    print("Data loaded successfully.")
    return nba_stats, nfl_stats, nba_odds, nfl_odds, nba_injuries, nfl_injuries

def determine_home_away(stats_df, odds_df, sport):
    """
    Determine home/away status for teams using the odds data.
    """
    print(f"Determining home/away status for {sport.upper()} teams...")

    # Standardize column names
    stats_df.columns = stats_df.columns.str.strip().str.lower()
    odds_df.columns = odds_df.columns.str.strip().str.lower()

    # Merge stats with odds on date and team
    stats_df = stats_df.rename(columns={"team": "team_name"})
    home_merge = stats_df.merge(
        odds_df[["date", "home_team"]],
        left_on=["date", "team_name"],
        right_on=["date", "home_team"],
        how="left"
    ).assign(home_status=lambda x: x["home_team"].notna().astype(int))

    away_merge = stats_df.merge(
        odds_df[["date", "away_team"]],
        left_on=["date", "team_name"],
        right_on=["date", "away_team"],
        how="left"
    ).assign(away_status=lambda x: x["away_team"].notna().astype(int))

    # Combine home and away status into one column
    stats_df["home"] = home_merge["home_status"]
    stats_df["away"] = away_merge["away_status"]
    stats_df["home"] = stats_df["home"].fillna(0).astype(int)
    stats_df["away"] = stats_df["away"].fillna(0).astype(int)

    print(f"Home/Away status determined for {sport.upper()} teams.")
    return stats_df

def compute_team_features(stats_df, sport):
    """
    Compute team-level features like rolling averages.
    """
    print(f"Computing team-level features for {sport.upper()}...")

    # Define the relevant column for points based on the sport
    if sport == "nba":
        points_column = "pts"  # Points for NBA
    elif sport == "nfl":
        points_column = "yds"  # Yards as a proxy for NFL points
    else:
        raise ValueError("Sport must be 'nba' or 'nfl'.")

    # Rolling average of points (or equivalent) scored (last 5 games)
    stats_df["avg_pts_last_5"] = stats_df.groupby("team_name")[points_column] \
        .rolling(window=5, min_periods=1).mean().reset_index(level=0, drop=True)

    print(f"Team-level features computed for {sport.upper()}.")
    return stats_df

if __name__ == "__main__":
    # Load data
    nba_stats, nfl_stats, nba_odds, nfl_odds, nba_injuries, nfl_injuries = load_data()

    # Feature engineering for NBA
    nba_features = determine_home_away(nba_stats, nba_odds, "nba")
    nba_features = compute_team_features(nba_features, "nba")
    nba_features.to_csv(os.path.join(FEATURES_DIR, "nba_features.csv"), index=False)
    print("NBA features saved to nba_features.csv.")

    # Feature engineering for NFL
    nfl_features = determine_home_away(nfl_stats, nfl_odds, "nfl")
    nfl_features = compute_team_features(nfl_features, "nfl")
    nfl_features.to_csv(os.path.join(FEATURES_DIR, "nfl_features.csv"), index=False)
    print("NFL features saved to nfl_features.csv.")

    print("\nFeature engineering complete!")
