import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import os
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import timezone

# Get the path to the directory one level higher than the current script
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Construct the full path to the players.db file
db_path = os.path.join(parent_dir, 'players.db')

# Define your timezone
wat_timezone = timezone('Africa/Lagos')

def fetch_team():
    """Fetches team data from the Premier League website and stores it in the database."""
    url = "https://www.premierleague.com/tables"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    target_div = soup.find('div', class_='league-table mainTableTab',
                           attrs={
                               'data-ui-tab': 'First Team',
                               'data-filter-list': 'FIRST',
                               'data-compseason': '719',
                               'data-detail': '2',
                               'data-live': 'true'
                           })

    data = []

    if target_div:
        team_rows = target_div.find_all('tr', {'class': ['tableDark', 'tableMid', 'tableLight', '']})

        for row in team_rows:
            position = row.find('span', class_='league-table__value value').text.strip()
            team_name = row.find('span', class_='league-table__team-name--long').text.strip()
            matches_played = row.find_all('td')[2].text.strip()
            wins = row.find_all('td')[3].text.strip()
            draws = row.find_all('td')[4].text.strip()
            losses = row.find_all('td')[5].text.strip()
            goals_for = row.find_all('td', class_='hideSmall')[0].text.strip()
            goals_against = row.find_all('td', class_='hideSmall')[1].text.strip()
            goal_difference = row.find_all('td')[7].text.strip()
            points = row.find('td', class_='league-table__points points').text.strip()

            data.append([position, team_name, matches_played, wins, draws, losses, goals_for, goals_against, goal_difference, points])

        df = pd.DataFrame(data, columns=[
            'Position', 'Team', 'Matches Played', 'Wins', 'Draws', 'Losses', 'Goals For', 'Goals Against', 'Goal Difference', 'Points'
        ])

        df.to_csv('premier_league_table.csv', index=False)
        print("Data saved to 'premier_league_table.csv'")

        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA foreign_keys = ON")

        # Fetch or create a snapshot entry
        cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
        snapshot_id = cursor.lastrowid

        for index, row in df.iterrows():
            """cursor.execute('''
                INSERT INTO Teams (snapshot_id, team_name, position, matches_played, wins, draws, losses, goals_for, goals_against, goal_difference, points, team_rating)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (snapshot_id, row['Team'], row['Position'], row['Matches Played'], row['Wins'], row['Draws'], row['Losses'], row['Goals For'], row['Goals Against'], row['Goal Difference'], row['Points'], 0))

        conn.commit()"""
            cursor.execute('''
                    UPDATE Teams
                    SET snapshot_id = ?, position = ?, matches_played = ?, wins = ?, draws = ?, losses = ?, goals_for = ?, goals_against = ?, goal_difference = ?, points = ?, team_rating = ?
                    WHERE team_name = ?
                ''', (snapshot_id, row['Team'], row['Position'], row['Matches Played'], row['Wins'], row['Draws'], row['Losses'], row['Goals For'], row['Goals Against'], row['Goal Difference'], row['Points'], 0))

        conn.commit()

        conn.close()

    else:
        print("The specific div could not be found.")


def update_team():
    """Schedules updates for the team data every 7 days."""
    scheduler = BackgroundScheduler(timezone=wat_timezone)

    # Schedule the update function to run every 7 days
    scheduler.add_job(fetch_team, 'interval', days=3, next_run_time=wat_timezone.localize(datetime.now()))

    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("Shutting down scheduler...")
        scheduler.shutdown()


if __name__ == "__main__":
    fetch_team()  # Initial data fetch
    update_team()  # Schedule updates