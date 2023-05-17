import pickle
import time
import os
from typing import List, Tuple
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from db import DB
import argparse 

"""
NOTE: Can't do this more than 20 times in one minute
or we get fked by rate limiting
"""
def parse_games(team: str, year: int, file = None):
    base_url = 'https://www.basketball-reference.com'
    
    # edge cases    
    if team == 'BRK' and year <= 2012: real_team = 'NJN'
    elif team == 'CHO' and year <= 2014: real_team = 'CHA'
    elif team == 'NOP' and year <= 2013: 
        print(f'Skipping pelicans {year}')
        return
    else: real_team = team

    if file: soup = BeautifulSoup(file, 'html.parser')
    else:
        url = f'{base_url}/teams/{real_team}/{year}_games.html'
        res = requests.get(url)
        if res.status_code == 429: 
            print(f'Got rate limited for {team} {year}...')
            exit(1)
        else: print(res.status_code)
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
            off_links = next_soup.find_all('a', href=lambda href: href and 'referee' in href)
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
    print(f'Finished {team} {year} with {real_games} games')

    time.sleep(60)


def write_to_db(team: str, year: int):
    file = open(f'pickles/{team}_{year}.pickle', 'rb')
    games, officials = pickle.load(file)
    file.close()
    with DB('db/nba.db') as db: db.insert_games(games, officials)


if __name__ == '__main__': 
    WEST_TEAMS = ['DEN', 'MEM', 'SAC', 'PHO', 'LAC', 'GSW', 'LAL', 'MIN', 'NOP', 'OKC', 'DAL', 'UTA', 'POR', 'HOU', 'SAS']
    EAST_TEAMS = ['MIL', 'BOS', 'PHI', 'CLE', 'NYK', 'BRK', 'MIA', 'ATL', 'TOR', 'CHI', 'IND', 'WAS', 'ORL', 'CHO', 'DET']

    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--region', choices=['east', 'west'], required=True)
    
    args = args.region
    if region == 'east': TEAMS = EAST_TEAMS
    else: TEAMS = WEST_TEAMS

    for team in TEAMS:
        for year in range(2012, 2022 + 1):
            if os.path.isfile(f'pickles/{team}_{year}.pickle'): 
                print(f'Skipping {team} {year}')
                continue
            else: parse_games(team, year)
