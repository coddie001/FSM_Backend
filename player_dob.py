from flask import Flask, jsonify
import requests
import sqlite3
from bs4 import BeautifulSoup
import re
from datetime import datetime
import time
import pandas as pd
app = Flask(__name__)

conn = sqlite3.connect('players.db')
cursor = conn.cursor()

# Select first name and second name from the players table
cursor.execute("SELECT first_name, second_name FROM players")
players = cursor.fetchall()
player_data = []

for player in players:
    first_name = player[0]
    second_name = player[1]
    full_name = f"{first_name} {second_name}"
    print(full_name)
    
    # Use the full name in the search URL
    url = f'https://www.google.com//search?q=google+{full_name}+dob&amp;sca_esv=4a9febc07b5fa96a&amp;ie=UTF-8&amp;source=lnt&amp;tbs=qdr:h&amp;sa=X&amp;ved=0ahUKEwjN48G5g-2HAxWHD7kGHWGbAfQQpwUIDA'
    
    # Send a GET request to the URL
    response = requests.get(url)

    # Check for successful response (status code 200)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Inspect the HTML and use the correct class name
        divs_with_header = soup.find_all('div', class_="BNeawe s3v9rd AP7Wnd")
        
        # Extract and print the text content from the found divs
        for div in divs_with_header:
            info_list = re.split(r"; Born:", div.text)
            dob = next((item for item in info_list if "Born:" in item), None)
            if dob:  # If dob is not None, print or append it
                print(dob)
                if dob:  # If dob is not None, clean and append it
                # Apply regex to extract the formatted date of birth
                    pattern = r"(\d{1,2})\s+([a-zA-Z]{3,})\s+(\d{4})"
                    dob_match = re.search(pattern, dob)
                    if dob_match:
                        dob = dob_match.group(0)  # Get the matched date string
                    else:
                        dob = None  # If no match is found, set dob to None

                    # Extract age using regex
                    age_match = re.search(r"age\s(\d{2})\s?[Āá†]?years", div.text)
                    age = age_match.group(1) if age_match else 'Unknown'

                    # Append the player's data
                    player_data.append((first_name, second_name, dob, age))

        # Pause for 5 seconds before processing the next player
        time.sleep(5)

# Creating DataFrame from the collected data
df = pd.DataFrame(player_data, columns=['first_name', 'second_name', 'dob', 'age'])

# Convert the 'dob' column to datetime, if possible
df['dob'] = pd.to_datetime(df['dob'], errors='coerce')

# Display the DataFrame
print(df)

for index, row in df.iterrows():
    cursor.execute('''
    UPDATE Players
    SET dob = ?, age = ?
    WHERE second_name = ?
    ''', (row['dob'], row['age'], row['second_name']))


conn.commit()
conn.close()

# Save the DataFrame to a CSV file
#df.to_csv('player_dob.csv', index=False)


################################### DataFrame and Store in DB 




















