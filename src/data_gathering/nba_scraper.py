import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw")

def scrape_nba_stats(year=2024):
    """Scrape NBA player statistics from Basketball Reference."""
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_totals.html"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to retrieve data for {year}")
        return

    soup = BeautifulSoup(response.content, "lxml")
    table = soup.find("table", {"id": "totals_stats"})

    # Convert the table to a pandas DataFrame
    df = pd.read_html(str(table))[0]
    df = df.dropna(subset=["Player"])  # Drop header duplicates
    df = df[df["Player"] != "Player"]  # Filter out repeated headers

    # Save to CSV
    output_path = os.path.join(OUTPUT_DIR, f"nba_stats_{year}.csv")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"NBA stats for {year} saved to {output_path}")

if __name__ == "__main__":
    for year in range(2000, 2025):  # Scrape data from 2000 to 2024
        scrape_nba_stats(year)
