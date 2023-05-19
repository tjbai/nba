import pickle
import sqlite3
from typing import List, Tuple

WEST_TEAMS = ['DEN', 'MEM', 'SAC', 'PHO', 'LAC', 'GSW', 'LAL', 'MIN', 'NOP', 'OKC', 'DAL', 'UTA', 'POR', 'HOU', 'SAS']
EAST_TEAMS = ['MIL', 'BOS', 'PHI', 'CLE', 'NYK', 'BRK', 'MIA', 'ATL', 'TOR', 'CHI', 'IND', 'WAS', 'ORL', 'CHO', 'DET']

class DB: 
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(self.db_file)
        self.cursor = self.conn.cursor()

    def __enter__(self): 
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.commit()
        self.conn.close()

    def execute(self, sql, params=()):
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def create_tables(self):
        self.cursor.execute('''
            CREATE TABLE Game (
                id INTEGER PRIMARY KEY,
                good_guys TEXT,
                bad_guys TEXT,
                gg_points INTEGER,
                bg_points INTEGER,
                win BOOLEAN,
                date TEXT
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE Game_Official (
                game_id INTEGER,
                official_id INTEGER,
                FOREIGN KEY (game_id) REFERENCES Game (id),
                FOREIGN KEY (official_id) REFERENCES Official (id)
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE Official (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE
            )
        ''')

    """
    Each game should be a tuple of shape:
    {
        'good_guys': str,
        'bad_guys': str,
        'gg_points': int,
        'bg_points': int,
        'win': boolean,
        'date': str,
    }

    and each official's name should be a string, 
    composed into a list corresponding to each game.
    """
    def insert_games(
        self, 
        games: List[Tuple[str, str, int, int, bool, str]], 
        game_officials: List[List[str]]
    ):
        game_query = '''
            INSERT INTO Game(good_guys, bad_guys, gg_points, bg_points, win, date)
            VALUES (?,?,?,?,?,?)
        '''

        join_query = '''
            INSERT INTO Game_Official (game_id, official_id)
            VALUES (?,?)
        '''

        official_query = '''
            INSERT INTO Official (name)
            Values (?)
            ON CONFLICT(name) DO UPDATE SET name=excluded.name
        '''

        try:
            self.conn.execute('BEGIN')

            for i, game in enumerate(games): 
                # Insert game row
                self.cursor.execute(game_query, game)
                gid = self.cursor.lastrowid

                # Upsert officials and return id
                officials = game_officials[i]
                off_ids = []
                for off in officials:
                    self.cursor.execute(official_query, (off,))
                    id = self.cursor.lastrowid
                    off_ids.append(id)

                # Insert into join table
                for oid in off_ids: self.cursor.execute(join_query, (gid, oid))
            
        except Exception as e:
            self.conn.rollback()
            print('Error inserting games: ', e)

    def test_insert_games(self):
        games = [
            ('LAL', 'GSW', 105, 100, True, '2022-05-13'),
            ('MIL', 'BOS', 98, 102, False, '2022-05-12')
        ]

        officials = [['TJ Bai', 'Test'], ['TJ Bai', 'John Doe']]

        self.insert_games(games, officials)


def init_table(): 
    with DB('db/nba.db') as db: db.create_tables()

def grab_pickle(team: str, year: int):
    try:
        file = open(f'pickles/{team}_{year}.pickle', 'rb')
        games, officials = pickle.load(file)
        return (games, officials)
    except Exception as e:
        print(e)
        return (None, None)

def write_to_db(team: str, year: int):
    file = open(f'pickles/{team}_{year}.pickle', 'rb')
    games, officials = pickle.load(file)
    file.close()
    with DB('db/nba.db') as db: db.insert_games(games, officials)

def team_year_pairs(start: int, end: int):
    for team in WEST_TEAMS + EAST_TEAMS:
        for year in range(start, end + 1):
            if team == 'NOP' and year <= 2013: continue 
            yield (team, year)

def check_pickles():
    sizes = {}
    for team, year in team_year_pairs(2012, 2022): 
        games, officials = grab_pickle(team, year)
        if games is None or officials is None: raise Exception(f'Unable to parse pickle for {team} {year}')

        game_len, off_len = len(games), len(officials)
        if game_len != off_len: raise Exception(f'{team} {year}: {game_len} != {off_len}')

        if game_len not in sizes: sizes[game_len] = []
        sizes[game_len].append((team, year))
    print(sizes.keys())

def write_pickles_to_db():
    for team, year in team_year_pairs(2012, 2022):
	pass

if __name__ == '__main__': 
    check_pickles()
