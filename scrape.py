import pickle
import time
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from db import DB

"""
NOTE: Can't do this more than 20 times in one minute
or we get fked by rate limiting
"""
def parse_games(team: str, year: int, file = None):
    base_url = 'https://www.basketball-reference.com'

    if file: soup = BeautifulSoup(file, 'html.parser')
    else:
        url = f'{base_url}/teams/{team}/{year}_games.html'
        res = requests.get(url)
        if res.status_code == 429: 
            print(f'Got rate limited for {team} {year}...')
            exit(1)
        soup = BeautifulSoup(res.content, 'html.parser')

    table = soup.find(id='games')
    games: List[Tuple[str, str, int, int, bool, str]] = []
    officials: List[List[str]] = []

    real_games = 0
    for row in tqdm(table.tbody):
        try:
            g = {'good_guys': team}
            offs = []
            next_url = None

            # Pull all the game statistics we care about
            for i, child in enumerate(row.children):
                if child['data-stat'] == 'date_game': 
                    g['date'] = child['csk']
                elif child['data-stat'] == 'pts': 
                    g['gg_points'] = child.text
                elif child['data-stat'] == 'opp_pts': 
                    g['bg_points'] = child.text
                elif child['data-stat'] == 'wins': 
                    g['win'] = (child.text == '1')
                elif child['data-stat'] == 'opp_name': 
                    link = child.find('a')
                    g['bad_guys'] = link['href'].split('/')[2]
                elif child['data-stat'] == 'box_score_text':
                    link = child.find('a')
                    next_url = f'{base_url}{link["href"]}'

            if next_url is None:
                raise Exception(f"Couldn't find follow-up href for {team}")

            # Pull all the official names
            next_res = requests.get(next_url)
            next_soup = BeautifulSoup(next_res.content, 'html.parser')
            off_links = next_soup.find_all('a', href= lambda href: href and 'referee' in href)
            for olink in off_links:
                if olink.text == 'Referees': break
                offs.append(olink.text)

            real_games += 1
            officials.append(offs)
            games.append((
                g['good_guys'],
                g['bad_guys'],
                g['gg_points'],
                g['bg_points'],
                g['win'],
                g['date']
            ))

            if real_games % 15 == 0: 
                print("Sleeping...")
                time.sleep(60)

        except (AttributeError, TypeError): continue
        except Exception as e:
            print(f'Error parsing games for {team} in {year}: ', e)
            print('Problematic row: ', row)
            # exit(1)

    with open(f'pickles/{team}_{year}.pickle', 'wb') as f: pickle.dump((games, officials), f)   
    print(f'Finished {team} {year}')

    time.sleep(60)


def write_to_db(team: str, year: int):
    file = open(f'pickles/{team}_{year}.pickle', 'rb')
    games, officials = pickle.load(file)
    file.close()
    with DB('db/nba.db') as db: db.insert_games(games, officials)


if __name__ == '__main__': 
    # We already did 2022 before. For each team, gonna do 2012-2022 inclusive.
    for i in range(2015, 2022): parse_games('MIA', i)
