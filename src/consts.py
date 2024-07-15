from espn_api_orm.consts import ESPNSportLeagueTypes


######################################
# Sport Consts
######################################

SEASON_GROUPS = {
    ESPNSportLeagueTypes.BASKETBALL_MENS_COLLEGE_BASKETBALL: {
        'dii/diii':51,
        'di':50
    },
    ESPNSportLeagueTypes.FOOTBALL_COLLEGE_FOOTBALL: {
        'dii/diii':35,
        'di':90
    },

    ESPNSportLeagueTypes.BASEBALL_COLLEGE_BASEBALL: {
        'di':26,
    },

    ESPNSportLeagueTypes.HOCKEY_MENS_COLLEGE_HOCKEY: None,
    ESPNSportLeagueTypes.LACROSSE_MENS_COLLEGE_LACROSSE: None,
    ESPNSportLeagueTypes.BASKETBALL_NBA: None,
    ESPNSportLeagueTypes.FOOTBALL_NFL: None,
    ESPNSportLeagueTypes.BASEBALL_MLB: None,
    ESPNSportLeagueTypes.HOCKEY_NHL: None,
    ESPNSportLeagueTypes.LACROSSE_PLL: None,
    ESPNSportLeagueTypes.SOCCER_ENG_1: None,
}

SEASON_START_MONTH = {
    ESPNSportLeagueTypes.BASKETBALL_MENS_COLLEGE_BASKETBALL: {'start': 10, 'wrap': True},
    ESPNSportLeagueTypes.FOOTBALL_COLLEGE_FOOTBALL: {'start': 7, 'wrap': False},
    ESPNSportLeagueTypes.BASEBALL_COLLEGE_BASEBALL: {'start': 1, 'wrap': False},
    ESPNSportLeagueTypes.HOCKEY_MENS_COLLEGE_HOCKEY: {'start': 10, 'wrap': True},
    ESPNSportLeagueTypes.LACROSSE_MENS_COLLEGE_LACROSSE: {'start': 1, 'wrap': False},
    ESPNSportLeagueTypes.BASKETBALL_NBA: {'start': 10, 'wrap': True},
    ESPNSportLeagueTypes.FOOTBALL_NFL: {'start': 9, 'wrap': False},
    ESPNSportLeagueTypes.BASEBALL_MLB: {'start': 4, 'wrap': False},
    ESPNSportLeagueTypes.HOCKEY_NHL: {'start': 10, 'wrap': True},
    ESPNSportLeagueTypes.LACROSSE_PLL: {'start': 6, 'wrap': True},
    ESPNSportLeagueTypes.SOCCER_ENG_1: {'start': 8, 'wrap': False},
}

START_SEASONS = {
    ESPNSportLeagueTypes.BASKETBALL_MENS_COLLEGE_BASKETBALL: 2002,
    ESPNSportLeagueTypes.FOOTBALL_COLLEGE_FOOTBALL: 2002,
    ESPNSportLeagueTypes.BASEBALL_COLLEGE_BASEBALL: 2015,
    ESPNSportLeagueTypes.HOCKEY_MENS_COLLEGE_HOCKEY: 2005,
    ESPNSportLeagueTypes.LACROSSE_MENS_COLLEGE_LACROSSE: 2008,
    ESPNSportLeagueTypes.BASKETBALL_NBA: 2000,
    ESPNSportLeagueTypes.FOOTBALL_NFL: 2002,
    ESPNSportLeagueTypes.BASEBALL_MLB: 2000,
    ESPNSportLeagueTypes.HOCKEY_NHL: 2000,
    ESPNSportLeagueTypes.LACROSSE_PLL: 2022,
    ESPNSportLeagueTypes.SOCCER_ENG_1: 2003,
}

