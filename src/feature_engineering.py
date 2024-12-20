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

def compute_team_features(stats_df, sport):
    """
    Compute team-level features like rolling averages from player-level data.
    """
    print(f"Aggregating player stats to compute team-level features for {sport.upper()}...")

    # Define the relevant columns for aggregation
    if sport == "nba":
        aggregation_columns = ["pts", "ast", "trb"]
    elif sport == "nfl":
        aggregation_columns = ["yds", "td"]
    else:
        raise ValueError("Sport must be 'nba' or 'nfl'.")

    # Aggregate player stats to team-level stats
    team_stats = stats_df.groupby(["date", "team"])[aggregation_columns].sum().reset_index()

    # Compute rolling averages for team-level stats
    for col in aggregation_columns:
        rolling_col = f"avg_{col}_last_5"
        team_stats[rolling_col] = team_stats.groupby("team")[col] \
            .rolling(window=5, min_periods=1).mean().reset_index(level=0, drop=True)

    print(f"Team-level features computed for {sport.upper()}.")
    return team_stats

def merge_team_with_player(player_stats_df, team_stats_df, sport):
    """
    Merge team-level features into player-level stats.
    """
    print(f"Merging team-level features into player-level data for {sport.upper()}...")

    # Merge team stats with player stats on date and team
    merged_stats = player_stats_df.merge(
        team_stats_df,
        on=["date", "team"],
        how="left"
    )

    # Drop the `home` and `away` columns if they exist
    merged_stats = merged_stats.drop(columns=["home", "away"], errors="ignore")

    print(f"Team-level features successfully merged into player-level data for {sport.upper()}.")
    return merged_stats

if __name__ == "__main__":
    # Load data
    nba_stats, nfl_stats, nba_odds, nfl_odds, nba_injuries, nfl_injuries = load_data()

    # Feature engineering for NBA
    nba_team_features = compute_team_features(nba_stats, "nba")
    nba_features = merge_team_with_player(nba_stats, nba_team_features, "nba")
    nba_features.to_csv(os.path.join(FEATURES_DIR, "nba_features.csv"), index=False)
    print("NBA features saved to nba_features.csv.")

    # Feature engineering for NFL
    nfl_team_features = compute_team_features(nfl_stats, "nfl")
    nfl_features = merge_team_with_player(nfl_stats, nfl_team_features, "nfl")
    nfl_features.to_csv(os.path.join(FEATURES_DIR, "nfl_features.csv"), index=False)
    print("NFL features saved to nfl_features.csv.")

    print("\nFeature engineering complete!")
