#!/usr/bin/python3.6
import config as cfg
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from googleapiclient.discovery import build
from google.oauth2 import service_account

# ----------------------------------------------------------
# CALENDAR CODE
# ----------------------------------------------------------
def format_date(date):
    """Formats a datetime formatted as RFC3339 timestamp with time zone offset, 
    as required by the Google Calendar API

    Parameters
    ----------
    date : datetime.date
        Date to be formatted

    Returns
    -------
    date_time_utc
        datetime.datetime
    """
    date_time = datetime.combine(date, datetime.min.time())
    date_time_utc = date_time.isoformat() + 'Z'
    return date_time_utc

def get_event_date(event, timepoint):
    """Extract date from an Google Calendar event

    Parameters
    ----------
    event : dict
        Google Calendar event
    timepoint : str
        specifies the type of date to extract, can be 'end' or 'start'

    Returns
    -------
    datetime
    """
    return event[timepoint].get('dateTime', event[timepoint].get('date'))

def unfold_events_list(events_list):
    """Returns a list of all extracted Google Calendar events. Multi-day events are split into separate days.
    The resulting list is limited to the days of the new week menu.

    Parameters
    ----------
    event_list : list
        list with Google Calendar events

    Returns
    -------
    new_events_list : list
        list with events for all days of the new week menu
    """
    new_events_list = []
    for e in events_list:
        start = datetime.strptime(e[0], '%Y-%m-%d').date()
        end = datetime.strptime(e[1], '%Y-%m-%d').date()
        delta_days = (end - start).days

        if delta_days > 1:
            for d in range(delta_days):
                unfolded_day = start + timedelta(days=d)
                if unfolded_day >= datetime.now().date() and unfolded_day <= datetime.now().date() + timedelta(days=6):
                    new_events_list.append((unfolded_day, e[2]))
        else:
            new_events_list.append((start, e[2]))
    return new_events_list

def get_events_by_calendarId(service, calendarId, timeMin, timeMax, allEvents):
    """Returns a list of all Google Calendar events of a specified calendarID.

    Parameters
    ----------
    service : object
        Google Calender API service object
    calendarId : str
        ID of the owner of the Google Calendar
    timeMin : datetime
        lower bound for an event's end time to filter by
    timeMin : datetime
        upper bound for an event's start time to filter by
    allEvents : list
        list of user-defined event labels to select

    Returns
    -------
    list
    """
    events_result = service.events().list(calendarId=calendarId
                                        , timeMin=timeMin
                                        , timeMax=timeMax
                                        , singleEvents=True
                                        , orderBy='startTime').execute()
    events = events_result.get('items', [])    
    events_list = [(get_event_date(e, 'start'), get_event_date(e, 'end'), e['summary'].upper()) 
                   for e in events 
                   if e['summary'].upper() in allEvents]
    return unfold_events_list(events_list)

def get_date_last_event(service, calendarId):
    """Returns the date of the last event of a Google Calendar

    Parameters
    ----------
    service : object
        Google Calender API service object
    calendarId : str
        ID of the owner of the Google Calendar

    Returns
    -------
    date_last_event : str
    """
    events_result = service.events().list(calendarId=calendarId
                                        , singleEvents=True
                                        , orderBy='startTime').execute()
    date_last_event = events_result.get('items', [])[-1]['start']['date'][:10]
    date_last_event = datetime.strptime(date_last_event, '%Y-%m-%d').date()
    return date_last_event

def create_events_df(events_list_1, events_list_2):
    """Returns a Pandas DataFrame containing the events of the two Google Calendars.

    Parameters
    ----------
    event_list_1 : list
        list with Google Calendar events
    event_list_2 : list
        list with Google Calendar events

    Returns
    -------
    events_df : Pandas DataFrame
        dataFrame with events of both Google Calendars indexed by the dates for the coming week
    """
    events_df_1 = pd.DataFrame.from_records(events_list_1, columns=['date', 'events_cal_1'])
    events_df_2 = pd.DataFrame.from_records(events_list_2, columns=['date', 'events_cal_2'])
    events_df = events_df_1.merge(events_df_2, on='date', how='outer')
    events_df.date = pd.to_datetime(events_df.date)
    events_df.set_index('date', inplace=True)
    events_df.sort_index(inplace=True)

    dates = list(pd.period_range(START_DAY, NEXT_WEEK, freq='D').values)
    new_idx = []
    for d in dates:
        new_idx.append(np.datetime64(d))

    events_df = events_df.reindex(new_idx)
    events_df.reset_index(inplace=True)
    events_df['weekday'] = events_df.date.apply(lambda x: x.strftime('%A'))
    events_df.set_index('date', inplace=True)
    return events_df

def add_weekmenu_to_calendar(service, weekmenu_df, calendarId):
    """Adds the selected recipes to a specified Google Calendar

    Parameters
    ----------
    service : object
        Google Calender API service object
    weekmenu_df : Pandas DataFrame
        dataFrame with recipes for the coming week
    calendarId : str
        ID of the owner of the Google Calendar
    """
    for i, r in weekmenu_df.iterrows():
        event = {
        'summary': r.recipe,
        'description': r.description,
        'start': {
            'date': i.date().isoformat(),
            'timeZone': 'Europe/Brussels'
        },
        'end': {
            'date': i.date().isoformat(),
            'timeZone': 'Europe/Brussels'
        }
        }
        event = service.events().insert(calendarId=calendarId, body=event).execute()


# ----------------------------------------------------------
# RECIPES CODE
# ----------------------------------------------------------
def get_recipes(service, spreadsheetId, range):
    """Returns a Pandas DataFrames containing (eligible) recipes for the week menu

    Parameters
    ----------
    service : object
        Google Calender Sheets service object
    spreadsheetId : str
        ID of the Google Spreadsheet
    range : str
        range within the Google Spreadsheet to extract the recipes

    Returns
    -------
    recipes_df : Pandas DataFrame
        dataFrame with all recipes in the Google SpreadSheet
    eligible_recipes : Pandas DataFrame
        dataFrame with recipes that were not used in the previous week menu
    """
    recipes_result = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range).execute()
    recipes = recipes_result.get('values', [])
    recipes_df = pd.DataFrame.from_records(recipes[1:], columns=recipes[0])
    recipes_df.last_date_on_menu = pd.to_datetime(recipes_df.last_date_on_menu, dayfirst=True)
    recipes_df.set_index('row_number', inplace=True)
    eligible_recipes = recipes_df[ (recipes_df.last_date_on_menu < PREV_WEEK) | (np.isnat(recipes_df.last_date_on_menu)) ]
    return recipes_df, eligible_recipes

def choose_recipe(difficulty, idx, weekmenu_df, eligible_recipes):
    """Returns an index of a recipe chosen for the week menu

    Parameters
    ----------
    difficulty : str
        difficulty of the recipe, can be 'easy', 'medium' or 'difficult'
    idx : datetime index
        index in the dataFrame that will contain the week menu and for which a recipe is chosen
    weekmenu_df : Pandas DataFrame
        dataFrame containing the week menu
    eligible_recipes : Pandas DataFrame
        dataFrame with recipes to choose from

    Returns
    -------
    choice_idx : datetime index
        index of the chosen recipe in the eligible_recipes dataFrame
    """
    choice_idx = np.random.choice(eligible_recipes.query("difficulty == 'difficult'" ).sort_values('last_date_on_menu', na_position='first').index.values[:5])
    weekmenu_df.loc[idx, 'recipe'] = eligible_recipes.loc[choice_idx, 'recipe']
    weekmenu_df.loc[idx, 'description'] = eligible_recipes.loc[choice_idx, 'description']
    eligible_recipes.drop(choice_idx, inplace=True)
    return choice_idx

def update_sheet(service, row_number, date, spreadsheetId):
    """Updates the last_date_on_menu in the Google SpreadSheet for the chosen recipe

    Parameters
    ----------
    service : object
        Google Calender Sheets service object
    row_number : int
        row number of the chosen recipe in the Google SpreadSheet
    date : date
        date to update last_date_on_menu with
    spreadsheetId : str
        ID of the Google Spreadsheet
    """
    range = "recepten!F"  + str(row_number)
    values = [[date]]
    body = {'values' : values}
    result = service.spreadsheets().values().update(spreadsheetId=spreadsheetId
                                                    , range=range
                                                    , valueInputOption='USER_ENTERED'
                                                    , body=body).execute()

def generate_weekmenu(service, events_df, traditions, free_events):
    """Generates week menu

    Parameters
    ----------
    service : object
        Google Calender Sheets service object
    events_df : Pandas DataFrame
        dataFrame with all events
    traditions : dict
        dictionary with recipes that must appear on a specific week day. Keys are the weekdays and values
        are the required recipe
    free_events : list
        list with event labels of free events

    Returns
    -------
    weekmenu_df : Pandas DataFrame
        dataFrame containing the chosen recipes
    """
    weekmenu_df = events_df.copy()

    for i, r in events_df.iterrows():
        if r.weekday in traditions.keys():
            weekmenu_df.loc[i, 'recipe'] = traditions[r.weekday]
            weekmenu_df.loc[i, 'description'] = ''
        else:
            if r.weekday in ['Saturday', 'Sunday']:
                row_number = choose_recipe('difficult', i, weekmenu_df, eligible_recipes)
                update_sheet(service, row_number, i.strftime('%d-%m-%Y'), cfg.SPREADSHEET_ID)
            elif r.events_cal_1 in free_events or r.events_cal_2 in free_events \
            or pd.isnull(r.events_cal_1) or pd.isnull(r.events_cal_2):
                row_number = choose_recipe('medium', i, weekmenu_df, eligible_recipes)
                update_sheet(service, row_number, i.strftime('%d-%m-%Y'), cfg.SPREADSHEET_ID)
            else:
                row_number = choose_recipe('easy', i, weekmenu_df, eligible_recipes)
                update_sheet(service, row_number, i.strftime('%d-%m-%Y'), cfg.SPREADSHEET_ID)
    return weekmenu_df


if __name__ == '__main__':
    # Getting credentials from credentials.json
    CREDS_PATH = Path.cwd() / "weekmenu" / "credentials.json"
    creds = service_account.Credentials.from_service_account_file(CREDS_PATH, scopes=cfg.SCOPES)

    # Creating service objects
    service_cal = build('calendar', 'v3', credentials=creds)
    service_sheet = build('sheets', 'v4', credentials=creds)

    # Defining dates
    DATE_LAST_RECIPE = get_date_last_event(service_cal, cfg.CALENDARID_WEEKMENU) 
    START_DAY = DATE_LAST_RECIPE + timedelta(days=1)
    NEXT_WEEK = START_DAY + timedelta(days=6)
    PREV_WEEK = START_DAY + timedelta(days=-7)
    START_DAY = format_date(START_DAY)
    NEXT_WEEK = format_date(NEXT_WEEK)
    PREV_WEEK = format_date(PREV_WEEK)

    # Getting the recipes from the Google Sheet
    recipes_df, eligible_recipes = get_recipes(service_sheet, cfg.SPREADSHEET_ID, cfg.RANGE)

    # Check if the last weekmenu is still active
    if DATE_LAST_RECIPE - timedelta(days=cfg.NB_DAYS_BEFORE) < datetime.now().date():
        # Getting the events from the Google Calendars
        events_list_1 = get_events_by_calendarId(service_cal, cfg.CALENDARID_1, START_DAY, NEXT_WEEK, cfg.ALL_EVENTS)
        events_list_2 = get_events_by_calendarId(service_cal, cfg.CALENDARID_2, START_DAY, NEXT_WEEK, cfg.ALL_EVENTS)

        # Merge the two events lists
        events_df = create_events_df(events_list_1, events_list_2)

        # Generating the weekmenu
        weekmenu_df = generate_weekmenu(service_sheet, events_df, cfg.TRADITIONS, cfg.FREE_EVENTS)

        # Adding the weekmenu to a Google Calendar
        add_weekmenu_to_calendar(service_cal, weekmenu_df, cfg.CALENDARID_WEEKMENU)
        print('Week menu is added to Google Calendar')
    else:
        print('Program stopped. Last week menu is not finished yet.')