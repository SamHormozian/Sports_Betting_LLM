import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw")

def scrape_injury_data_with_headers(sport):
    """
    Scrape injury data for NFL or NBA using headers to mimic a browser.

    Args:
        sport (str): 'nfl' or 'nba'
    """
    base_url = f"https://www.espn.com/{sport}/injuries"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(base_url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve injury data for {sport}. Status Code: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    injury_table = soup.find("table", {"class": "Table"})

    if not injury_table:
        print(f"No injury table found for {sport}.")
        return

    # Extract table data into a DataFrame
    df = pd.read_html(str(injury_table))[0]

    # Save the DataFrame to CSV
    output_path = os.path.join(OUTPUT_DIR, f"{sport}_injuries.csv")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"Injury data for {sport} saved to {output_path}")


if __name__ == "__main__":
    scrape_injury_data_with_headers("nfl")  # Scrape NFL injuries
    scrape_injury_data_with_headers("nba")  # Scrape NBA injuries
