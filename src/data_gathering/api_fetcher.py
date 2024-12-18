import requests
import pandas as pd
import os

API_KEY = "cccdf24248e03db3e9d8dd1578a2fea3"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw")


def fetch_odds(sport="basketball_nba", regions="us", markets=["spreads", "totals", "h2h"]):
    """
    Fetch betting odds from TheOddsAPI for specified markets.

    Args:
        sport (str): The sport key (e.g., 'basketball_nba' or 'americanfootball_nfl').
        regions (str): Regions to include (default is 'us').
        markets (list): List of markets to fetch (e.g., 'spreads', 'totals', 'h2h').
    """
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds/"
    params = {
        "apiKey": API_KEY,
        "regions": regions,
        "markets": ",".join(markets),
        "oddsFormat": "decimal"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print("Failed to fetch odds:", response.json())
        return

    data = response.json()

    # Process data into a structured format
    results = []
    for game in data:
        home_team = game.get("home_team")
        away_team = game.get("away_team")
        commence_time = game.get("commence_time")
        bookmakers = game.get("bookmakers", [])

        for bookmaker in bookmakers:
            bookmaker_name = bookmaker.get("title")
            markets = bookmaker.get("markets", [])

            for market in markets:
                market_type = market.get("key")  # 'spreads', 'totals', 'h2h'
                outcomes = market.get("outcomes", [])

                for outcome in outcomes:
                    results.append({
                        "commence_time": commence_time,
                        "home_team": home_team,
                        "away_team": away_team,
                        "bookmaker": bookmaker_name,
                        "market_type": market_type,
                        "name": outcome.get("name"),  # Team or total
                        "price": outcome.get("price"),  # Decimal odds
                        "point": outcome.get("point")  # Spread/total line (if applicable)
                    })

    # Convert to DataFrame and save
    df = pd.DataFrame(results)
    output_path = os.path.join(OUTPUT_DIR, f"{sport}_odds.csv")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Odds data saved to {output_path}")


if __name__ == "__main__":
    fetch_odds(sport="basketball_nba")  # Fetch NBA odds
    fetch_odds(sport="americanfootball_nfl")  # Fetch NFL odds
