import json
import os

import pandas as pd
from espn_api_orm.event.api import ESPNEventAPI
from espn_api_orm.event.schema import Event
from espn_api_orm.league.api import ESPNLeagueAPI
from espn_api_orm.scoreboard.api import ESPNScoreboardAPI
from espn_api_orm.calendar.api import ESPNCalendarAPI
from espn_api_orm.consts import ESPNSportLeagueTypes, ESPNSportSeasonTypes

import requests
import datetime

from src.consts import START_SEASONS
from src.utils import get_seasons_to_update, put_json_file, get_dataframe, find_year_for_season, put_dataframe


def _process_event_and_roster(event_obj):
    """
    Returns a single event dictionary and a list of individual players on roster
    :param event_obj:
    :return:
    """
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
        'away_win_prob': None,
        'home_win_prob': None,
        'away_chance_loss': None,
        'home_chance_loss': None,
        'away_chance_tie': None,
        'home_chance_tie': None,
        'away_matchup_quality': None,
        'home_matchup_quality': None,
        'away_opp_season_strength_fbs_rank': None,
        'home_opp_season_strength_fbs_rank': None,
        'away_opp_season_strength_rating': None,
        'home_opp_season_strength_rating': None,
        'away_pred_point_diff': None,
        'home_pred_point_diff': None,

    }

    if event_obj.predictor is not None:
        for team in ['home', 'away']:
            for predictor_stat in event_obj.predictor[f'{team}Team']['statistics']:
                if predictor_stat['name'] == 'gameProjection':
                    processed_event[f'{team}_win_prob'] = predictor_stat['value'] if predictor_stat['value'] is not None else float(predictor_stat['displayValue'])
                elif predictor_stat['name'] == 'matchupQuality':
                    processed_event[f'{team}_matchup_quality'] = predictor_stat['value'] if predictor_stat['value'] is not None else float(predictor_stat['displayValue'])
                elif predictor_stat['name'] == 'oppSeasonStrengthFbsRank':
                    processed_event[f'{team}_opp_season_strength_fbs_rank'] = predictor_stat['value'] if predictor_stat['value'] is not None else float(predictor_stat['displayValue'])
                elif predictor_stat['name'] == 'oppSeasonStrengthRating':
                    processed_event[f'{team}_opp_season_strength_rating'] = predictor_stat['value'] if predictor_stat['value'] is not None else float(predictor_stat['displayValue'])
                elif predictor_stat['name'] == 'teamChanceLoss':
                    processed_event[f'{team}_chance_loss'] = predictor_stat['value'] if predictor_stat['value'] is not None else float(predictor_stat['displayValue'])
                elif predictor_stat['name'] == 'teamChanceTie':
                    processed_event[f'{team}_chance_tie'] = predictor_stat['value'] if predictor_stat['value'] is not None else float(predictor_stat['displayValue'])
                elif predictor_stat['name'] == 'teamPredPtDiff':
                    processed_event[f'{team}_pred_point_diff'] = predictor_stat['value'] if predictor_stat['value'] is not None else float(predictor_stat['displayValue'])


    processed_roster = []
    if away_team.team.athletes is not None:
        for away_player in away_team.team.athletes:
            processed_roster.append(
                {
                    'event_id': event_obj.id,
                    'player_id': away_player['playerId'],
                    'team_id': away_team.team.id,
                    'team_abbr': away_team.team.abbreviation,
                    'period': away_player['period'],
                    'active': away_player['active'],
                    'starter': away_player['starter'],
                    'did_not_play': away_player['didNotPlay'],
                }
            )
    if home_team.team.athletes is not None:
        for home_player in home_team.team.athletes:
            processed_roster.append(
                {
                    'event_id': event_obj.id,
                    'player_id': home_player['playerId'],
                    'team_id': home_team.team.id,
                    'team_abbr': home_team.team.abbreviation,
                    'period': home_player['period'],
                    'active': home_player['active'],
                    'starter': home_player['starter'],
                    'did_not_play': home_player['didNotPlay'],
                }
            )


    return processed_event, processed_roster


if __name__ == '__main__':
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
            season_path = f"{path}{update_season}/"
            if not os.path.exists(season_path):
                os.makedirs(season_path, exist_ok=True)

            processed_df = get_dataframe(f"{processed_events_path}{update_season}.parquet")
            if processed_df.shape[0] != 0:
                processed_rosters_df = get_dataframe(f"{processed_rosters_path}{update_season}.parquet")
                ## Only keeping cached games where we have the event and roster data
                processed_df = processed_df[processed_df.id.isin(processed_rosters_df.event_id.unique())].copy()

            calendar_api = ESPNCalendarAPI(sport_str, league_str, update_season)
            season_types = calendar_api.get_valid_types()

            calendar_sections = calendar_api.get_calendar_sections(season_types)

            if update_season == find_year_for_season(sport_league) and processed_df.shape[0] != 0:
                ### ADD in last run date from processed and go 2 days back
                last_valid_date = pd.Timestamp(pd.Timestamp(processed_df[((processed_df['status']=='3'))].date.max()).to_pydatetime() - datetime.timedelta(days=7))
                for calendar_section in calendar_sections:
                    on_days = calendar_section.dates
                    calendar_section.dates = [day for day in on_days if pd.Timestamp(last_valid_date).to_pydatetime() <= day]
                existing_games_for_season = list(processed_df[((processed_df['date'] <= last_valid_date) & (processed_df['status']=='3'))].id.values)
            else:
                existing_games_for_season = [i.split('.')[0] for i in os.listdir(season_path)]

            for calendar_section in calendar_sections:
                for date in calendar_section.dates:
                    season_type = calendar_section.seasonType
                    string_date = f"{(date + datetime.timedelta(days=-1)).strftime('%Y%m%d')}-{date.strftime('%Y%m%d')}"
                    scoreboard_api = ESPNScoreboardAPI(sport_str, league_str)
                    scoreboard_obj = scoreboard_api.get_scoreboard(string_date)
                    for event in scoreboard_obj.events:
                        event_obj = event
                        event_obj_id = str(event_obj.id)

                        ## event is not finished or event is within the past 7 days
                        if event_obj_id in existing_games_for_season or event.season.type != season_type.value:
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

        ## Check what seasons need to get updated
        update_seasons = get_seasons_to_update("./events", sport_league)

        print(f"Running Processed Pump for: {sport_league.value} from {min(update_seasons)}-{max(update_seasons)}")
        for update_season in update_seasons:
            season_path = f"{path}{update_season}/"

            existing_games_for_season = [i.split('.')[0] for i in os.listdir(season_path)]
            events = []
            rosters = []
            for event_id in existing_games_for_season:
                file_path = os.path.join(season_path, f"{event_id}.json")
                with open(file_path, 'r') as file:
                    event_obj = Event.model_validate_json(json.load(file))
                    if event_obj.season.type not in [ESPNSportSeasonTypes.REG.value, ESPNSportSeasonTypes.POST.value]:
                        file.close()
                        os.remove(file_path)
                    else:
                        processed_event, processed_roster = _process_event_and_roster(event_obj)
                        events.append(processed_event)
                        rosters.extend(processed_roster)

            events_df = pd.DataFrame(events)
            rosters_df = pd.DataFrame(rosters)
            put_dataframe(events_df, f"{processed_events_path}{update_season}.parquet")
            put_dataframe(rosters_df, f"{processed_rosters_path}{update_season}.parquet")
