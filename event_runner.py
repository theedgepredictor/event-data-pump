import json
import os

from espn_api_orm.event.api import ESPNEventAPI
from espn_api_orm.event.schema import Event
from espn_api_orm.league.api import ESPNLeagueAPI
from espn_api_orm.scoreboard.api import ESPNScoreboardAPI
from espn_api_orm.calendar.api import ESPNCalendarAPI
from espn_api_orm.consts import ESPNSportLeagueTypes, ESPNSportSeasonTypes

import requests
import datetime

from src.consts import START_SEASONS
from src.utils import get_seasons_to_update, put_json_file, get_dataframe, find_year_for_season


def _process_event_and_roster(event_obj):
    competition = event_obj.competitions[0]

    # Handle Home and Away team selection from competitors
    if competition.competitors[0].homeAway == 'home':
        home_team_idx = 0
        away_team_idx = 1
    else:
        home_team_idx = 0
        away_team_idx = 1

    home_team = competition.competitors[home_team_idx]
    away_team = competition.competitors[away_team_idx]
    try:
        home_score = int(home_team.score) if home_team.score is not None else None
        away_score = int(away_team.score) if away_team.score is not None else None
    except Exception as e:
        home_score = None
        away_score = None


    if event_obj.predictor is not None:
        home_win_prob = event_obj.predictor
    else:
        home_win_prob = None

    processed_event = {
        'id': event_obj.id,
        'season': event_obj.season.year,
        'season_type': ESPNSportSeasonTypes(event_obj.season.type).value,
        'date': event_obj.date,
        'status': event_obj.status.type.id,
        'name': event_obj.name,
        'attendance': competition.attendance,
        'conference_competition': competition.conferenceCompetition,
        'event_venue_id': competition.venue.id if competition.venue is not None else None,

        'away_team_id': away_team.team.id,
        'home_team_id': home_team.team.id,
        'away_team_venue_id': away_team.team.venue.id if away_team.team.venue is not None else None,
        'home_team_venue_id': home_team.team.venue.id if home_team.team.venue is not None else None,
        'away_score': away_score,
        'home_score': home_score,
        'away_abbr': away_team.team.abbreviation,
        'home_abbr': home_team.team.abbreviation,
    }

    return processed_event, event_obj


def raw_scrape_pump():
    root_path = './raw'

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
        path = f'{root_path}/{sport_str}/{league_str}/'
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        league_api = ESPNLeagueAPI(sport_str, league_str)
        ## Check if league is active
        if not league_api.is_active():
            continue

        processed_events_path = f'./events/{sport_str}/{league_str}/'
        if not os.path.exists(processed_events_path):
            os.makedirs(processed_events_path, exist_ok=True)

        processed_rosters_path = f'./rosters/{sport_str}/{league_str}/'
        if not os.path.exists(processed_rosters_path):
            os.makedirs(processed_rosters_path, exist_ok=True)

        ## Check what seasons need to get updated
        update_seasons = get_seasons_to_update(root_path, sport_league)

        print(f"Running Raw Pump for: {sport_league.value} from {min(update_seasons)}-{max(update_seasons)}")

        for update_season in update_seasons:
            season_path = f"{path}/{update_season}/"
            if not os.path.exists(season_path):
                os.makedirs(season_path, exist_ok=True)

            processed_df = get_dataframe(f"{processed_events_path}{update_season}.parquet")

            if update_season != find_year_for_season(ESPNSportLeagueTypes.FOOTBALL_NFL):
                ## From processed
                existing_games_for_season = [i.split('.')[0] for i in os.listdir(season_path)]
            else:
                existing_games_for_season = processed_df

            calendar_api = ESPNCalendarAPI(sport_str, league_str, update_season)
            season_types = calendar_api.get_valid_types()
            calendar_sections = calendar_api.get_calendar_sections(season_types)

            ### ADD in last run date from processed and go 2 days back

            for calendar_section in calendar_sections:
                for date in calendar_section.dates:
                    string_date = f"{(date + datetime.timedelta(days=-1)).strftime('%Y%m%d')}-{date.strftime('%Y%m%d')}"
                    scoreboard_api = ESPNScoreboardAPI(sport_str, league_str)
                    scoreboard_obj = scoreboard_api.get_scoreboard(string_date)
                    for event in scoreboard_obj.events:
                        event_obj = event
                        event_obj_id = str(event_obj.id)

                        ## Or event is not finished or event is within the past 5 days
                        if event_obj_id in existing_games_for_season:
                            continue

                        event_api = ESPNEventAPI(sport_str, league_str, event_obj.id)
                        team1 = event_obj.competitions[0].competitors[0]
                        team1_roster = event_api.get_roster(team1.id)
                        if team1_roster is not None:
                            event_obj.competitions[0].competitors[0].team.athletes = team1_roster['entries']

                        team2 = event_obj.competitions[0].competitors[1]
                        team2_roster = event_api.get_roster(team2.id)
                        if team2_roster is not None:
                            event_obj.competitions[0].competitors[1].team.athletes = team2_roster['entries']

                        event_obj.predictor = event_api.get_prediction()

                        json_event = event.model_dump_json()
                        existing_games_for_season.extend([event_obj_id])
                        put_json_file(f"{season_path}{event_obj_id}.json",json_event)
            '''
            for event_id in existing_games_for_season:
                file_path = os.path.join(season_path, f"{event_id}.json")
                with open(file_path, 'r') as file:
                    event_obj = Event.model_validate_json(json.load(file))
                    processed_event, processed_roster = _process_event_and_roster(event_obj)
            '''


if __name__ == '__main__':
    raw_scrape_pump()


