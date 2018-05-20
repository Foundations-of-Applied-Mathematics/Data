# get_ncaa_data.py
"""Get win/lose NCAA men's basketball data from www.sports-reference.com."""

import time
import datetime
import pandas as pd
from tqdm import tqdm


SITE = "https://www.sports-reference.com/cbb/boxscores/index.cgi?month={}&day={}&year={}"

def get_win_lose_tuple(table):
    """Extract the (winner,loser) pair in a given game."""
    team1,team2 = [t.split("\xa0")[0] for t in table[0]]
    score1,score2 = table[1]
    return (team1, team2) if score1 > score2 else (team2, team1)

def main(season=None):
    """Get a list of (winner,loser) pairs for every NCAA men's basketball game
    in the specified season (season==2010 means the 2010-2011 seaon).
    """
    today = datetime.datetime.today().date()
    if not season:
        # Figure out what season it is.
        season = today.year - 1 if today.month < 10 else today.year
        print("Getting data for the {}-{} season".format(season, season+1))
    season = int(season)

    # Get the list of pages to scrape.
    pages = []
    start_date = datetime.date(season, 10, 25)  # October 25th, before season
    end_date = datetime.date(season+1, 4, 20)   # April 20th, after season
    end_date = min(end_date, today)             # Don't try to see the future.
    for n_days in range((end_date - start_date).days + 1):
        date = start_date + datetime.timedelta(days=n_days)
        pages.append(SITE.format(date.month, date.day, date.year))

    # Scrape each page.
    games = []
    try:
        for page in tqdm(pages):
            time.sleep(1)
            try:
                tables = pd.read_html(page) # PANDAS MAGIC!!
                games.extend([get_win_lose_tuple(t) for t in tables])
            except ValueError as e:
                # Ignore the error "there weren't games that day."
                if e.args[0] == "No tables found":
                    continue
                else:
                    print(type(e).__name__ + ':', e)
                    raise
    finally:
        # Export the data.
        df = pd.DataFrame(games, columns=["Winner", "Loser"])
        df.to_csv("ncaa{}.csv".format(season), index=False)

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 2:
        main(sys.argv[-1])
