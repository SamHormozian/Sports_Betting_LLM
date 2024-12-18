import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw")

def scrape_nfl_stats(year=2024):
    """Scrape NFL player statistics from Pro Football Reference."""
    url = f"https://www.pro-football-reference.com/years/{year}/passing.htm"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to retrieve data for {year}")
        return

    soup = BeautifulSoup(response.content, "lxml")
    table = soup.find("table", {"id": "passing"})

    # Convert the table to a pandas DataFrame
    df = pd.read_html(str(table))[0]
    df = df.dropna(subset=["Player"])
    df = df[df["Player"] != "Player"]  # Filter out repeated headers

    # Save to CSV
    output_path = os.path.join(OUTPUT_DIR, f"nfl_stats_{year}.csv")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"NFL stats for {year} saved to {output_path}")

if __name__ == "__main__":
    for year in range(2000, 2025):  # Scrape data from 2000 to 2024
        scrape_nfl_stats(year)
