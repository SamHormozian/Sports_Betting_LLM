import sys
import os

# Dynamically add the `src` directory to Python's module search path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.insert(0, project_root)

from data_gathering.nba_scraper import scrape_nba_stats
from data_gathering.nfl_scraper import scrape_nfl_stats
from data_gathering.api_fetcher import fetch_odds
from data_gathering.injury_scraper import scrape_injury_data_with_headers

def run_pipeline():
    print("Starting data gathering pipeline...")

    # Scrape player statistics
    for year in range(2020, 2025):
        print(f"Scraping NBA stats for {year}...")
        scrape_nba_stats(year)
        print(f"Scraping NFL stats for {year}...")
        scrape_nfl_stats(year)

    # Fetch betting odds
    print("Fetching NBA betting odds...")
    fetch_odds(sport="basketball_nba")
    print("Fetching NFL betting odds...")
    fetch_odds(sport="americanfootball_nfl")

    # Scrape injury data
    print("Scraping NFL injuries...")
    scrape_injury_data_with_headers("nfl")
    print("Scraping NBA injuries...")
    scrape_injury_data_with_headers("nba")

    print("Data gathering pipeline complete!")

if __name__ == "__main__":
    run_pipeline()
