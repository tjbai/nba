from typing import List, Tuple, Union

from db import DB

DBP = 'db/nba.db'

def team_win_loss_with_ref(
    team: str, 
    off: Union[int, str], 
    start: str = None, 
    end: str = None,
    season: Union[int, Tuple[int, int]] = None, # typescript is rubbing off
):
    with DB(DBP) as db:
        if season is not None and isinstance(season, int):
            start_date, end_date = db.get_start_end(team, season)
        elif season is not None and isinstance(season, tuple):
            start_date, _ = db.get_start_end(team, season[0])
            _, end_date = db.get_start_end(team, season[1])
        elif start is not None and end is not None:
            start_date, end_date = start, end
        else: raise TypeError()

        if isinstance(off, int): 
            oname = db.get_official_from_id(off)
        elif isinstance(off, str): 
            oname = off

        query = '''
            SELECT COUNT(*) as total_games,
                SUM(CASE WHEN win THEN 1 ELSE 0 END) as total_wins
            FROM Game
            INNER JOIN Game_Official ON Game.id = Game_Official.game_id
            INNER JOIN Official ON Game_Official.official_id = Official.id
            WHERE date BETWEEN ? AND ?
            AND (good_guys = ?)
            AND Official.name = ?
        '''
        params = (start_date, end_date, team, oname)
        res = db.execute(query, params)

        if len(res) == 0: return None
        total_games, total_wins = res[0]
        return (total_wins, total_games - total_wins)

def get_historic_officiating_team(good_guys: str, bad_guys: str, date: str) -> List[str]:
    with DB(DBP) as db:
        query = '''
            SELECT O.name
            FROM Game G
            INNER JOIN Game_Official GO ON G.id = GO.game_id
            INNER JOIN Official O ON GO.official_id = O.id
            WHERE G.good_guys = ? AND G.bad_guys = ? AND G.date = ?
        '''
        params = (good_guys, bad_guys, date)
        res = db.execute(query, params)

        if len(res) == 0:
            print("No matching game found")
            exit(1) 
        return [r[0] for r in res]

def get_games():
    pass

if __name__ == '__main__':
    # res = team_win_loss_with_ref('DEN', 1, season=('2012', '2014'))
    # print(res)
    res = get_historic_officiating_team('DEN', 'DAL', '2011-12-26')
    print(res)
