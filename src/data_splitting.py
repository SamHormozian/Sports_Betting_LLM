import pandas as pd
import os
from sklearn.model_selection import train_test_split

# Directories
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FEATURES_DIR = os.path.join(PROJECT_ROOT, "data", "features")
SPLIT_DIR = os.path.join(PROJECT_ROOT, "data", "splits")
os.makedirs(SPLIT_DIR, exist_ok=True)

# Files
NBA_FEATURES_FILE = os.path.join(FEATURES_DIR, "nba_features.csv")
NFL_FEATURES_FILE = os.path.join(FEATURES_DIR, "nfl_features.csv")

def create_targets(data, sport):
    """
    Create target columns for game outcomes, spread, and over/under.
    """
    print(f"Creating targets for {sport.upper()}...")

    if sport == "nba":
        points_column = "pts_y"  # Team-level points
    elif sport == "nfl":
        points_column = "yds_y"  # Team-level yards
    else:
        raise ValueError("Sport must be 'nba' or 'nfl'.")

    # Target 1: Game Outcome (1 if team scores above average for the day)
    data["game_outcome"] = (data[points_column] > data.groupby("date")[points_column].transform("mean")).astype(int)

    # Drop rows with missing targets
    data = data.dropna(subset=["game_outcome"])

    print(f"Targets created for {sport.upper()}.")
    return data

def split_data(data, sport):
    """
    Split data into training, validation, and testing sets.
    """
    print(f"Splitting data for {sport.upper()}...")

    train, test = train_test_split(data, test_size=0.3, random_state=42)
    val, test = train_test_split(test, test_size=0.5, random_state=42)

    # Save splits
    train.to_csv(os.path.join(SPLIT_DIR, f"{sport}_train.csv"), index=False)
    val.to_csv(os.path.join(SPLIT_DIR, f"{sport}_val.csv"), index=False)
    test.to_csv(os.path.join(SPLIT_DIR, f"{sport}_test.csv"), index=False)

    print(f"Data split for {sport.upper()} saved: train, val, test.")

if __name__ == "__main__":
    # NBA
    print("Processing NBA data...")
    nba_data = pd.read_csv(NBA_FEATURES_FILE)
    nba_data = create_targets(nba_data, "nba")
    split_data(nba_data, "nba")

    # NFL
    print("Processing NFL data...")
    nfl_data = pd.read_csv(NFL_FEATURES_FILE)
    nfl_data = create_targets(nfl_data, "nfl")
    split_data(nfl_data, "nfl")

    print("\nData splitting and target creation complete!")
