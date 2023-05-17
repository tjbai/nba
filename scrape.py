import argparse
import os
import pickle
import time
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from db import DB, EAST_TEAMS, WEST_TEAMS


"""
Autoregulates requests to prevent more than a certain amount
from being made within a select timeframe. I was just sleeping 
the thread before but this way can save as much as a minute
for every year we parse. That adds up across 30 teams * 20 years.
"""
class RequestLimiter:
    def __init__(self, request_limit, blocking_time):
        self.request_limit = request_limit
        self.blocking_time = blocking_time 
        self.request_count = 0 
        self.last_request_time = time.time()

    def get(self, url, **kwargs):
        current_time = time.time()

        if current_time - self.last_request_time > self.blocking_time:
            print('Enough time has elapsed since last request, resetting count')
            self.request_count = 0
            self.last_request_time = current_time

        if self.request_count >= self.request_limit:
            remaining_time = self.blocking_time - (current_time - self.last_request_time)
            print(f'Reached request limit, sleeping thread for {remaining_time + 10} seconds')
            if remaining_time > 0: time.sleep(remaining_time + 10)
            self.request_count = 0
            self.last_request_time = time.time()

        self.request_count += 1
        return requests.get(url, **kwargs)

"""
NOTE: Can't do this more than 20 times in one minute
or we get fked by rate limiting
"""
def parse_games(team: str, year: int, file = None, limiter: RequestLimiter = None):
    print(f'\nStarting {team} {year}')
    base_url = 'https://www.basketball-reference.com'
    
    # Edge cases as recent as 2012
    if team == 'BRK' and year <= 2012: real_team = 'NJN'
    elif team == 'CHO' and year <= 2014: real_team = 'CHA'
    else: real_team = team

    if file: soup = BeautifulSoup(file, 'html.parser')
    else:
        url = f'{base_url}/teams/{real_team}/{year}_games.html'
        # Tricky tricky!
        res = limiter.get(url) if limiter is not None else requests.get(url)
        if res.status_code == 429: 
            print(f'Got rate limited for {team} {year}...')
            exit(1)
        else: print(f'Status code: {res.status_code}')
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
                elif child['data-stat'] == 'game_streak': 
                    g['win'] = (child.text[0] == 'W')
                elif child['data-stat'] == 'opp_name': 
                    link = child.find('a')
                    g['bad_guys'] = link['href'].split('/')[2]
                elif child['data-stat'] == 'box_score_text':
                    link = child.find('a')
                    next_url = f'{base_url}{link["href"]}'

            if next_url is None: raise Exception(f"Couldn't find follow-up href for {team}")

            # Pull all the official names
            next_res = limiter.get(next_url) if limiter is not None else requests.get(next_url) # tricky tricky!
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

            # Sleep the thread greedily if no limiter
            if real_games % 15 == 0 and limiter is None: 
                print("Sleeping...")
                time.sleep(60)

        except (AttributeError, TypeError): continue
        except Exception as e:
            print(f'Error parsing games for {team} in {year}: ', e)
            print('Problematic row: ', row)

    with open(f'pickles/{team}_{year}.pickle', 'wb') as f: pickle.dump((games, officials), f)   
    print(f'Finished {team} {year} with {real_games} games')

    if limiter is None: time.sleep(60)


if __name__ == '__main__': 
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-s', '--side', choices=['E', 'W'], required=True)
    # parser.add_argument('-y', '--years', nargs=2, type=int, metavar=('START', 'END'), required=True)

    # args = parser.parse_args()
    # side = args.side
    # start_year, end_year = args.years

    # # real params should be 20 and 60
    # limiter = RequestLimiter(request_limit=19, blocking_time=70) 
    # for team in EAST_TEAMS if side == 'E' else WEST_TEAMS:
    #     for year in range(start_year, end_year + 1):
    #         if os.path.isfile(f'pickles/{team}_{year}.pickle'): print(f'Skipping {team} {year}')
    #         elif team == 'NOP' and year <= 2013: print(f'Skipping pelicans {year}')
    #         else: parse_games(team, year, limiter=limiter)

    limiter = RequestLimiter(request_limit=19, blocking_time=70)
    parse_games('MIA', 2016, limiter=limiter)
    parse_games('MIA', 2017, limiter=limiter)
    parse_games('MIA', 2018, limiter=limiter)
    parse_games('MIA', 2019, limiter=limiter)

