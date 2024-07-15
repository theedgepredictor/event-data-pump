import json
import os
from espn_api_orm.league.api import ESPNLeagueAPI
from espn_api_orm.season.api import ESPNSeasonAPI
from espn_api_orm.calendar.api import ESPNCalendarAPI
from espn_api_orm.consts import ESPNSportLeagueTypes

import requests
import datetime

def main():
    root_path = './data'

    sport_league_pairs = list(ESPNSportLeagueTypes)
    sport_league_pairs = [
        ESPNSportLeagueTypes.FOOTBALL_NFL,
        #ESPNSportLeagueTypes.FOOTBALL_COLLEGE_FOOTBALL,
        #ESPNSportLeagueTypes.BASKETBALL_MENS_COLLEGE_BASKETBALL,
        #ESPNSportLeagueTypes.BASKETBALL_NBA,
        #ESPNSportLeagueTypes.BASKETBALL_WNBA,
        #ESPNSportLeagueTypes.BASKETBALL_WOMENS_COLLEGE_BASKETBALL,
    ]
    for sport_league in sport_league_pairs:
        sport_str, league_str = sport_league.value.split('/')
        path = f'{root_path}/events/{sport_str}/{league_str}/'
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        league_api = ESPNLeagueAPI(sport_str, league_str)
        ## Check if league is active
        if not league_api.is_active():
            continue

        ## Check what seasons need to get updated

        ##




if __name__ == '__main__':
    main()