# EVERYTHING IMPORTED!

import json
import os
from datetime import datetime, timedelta
from dags.utils.tab_studio_utils import get_access_token
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import numpy as np
from dags.utils.api_utils import get_json_from_api, turn_json_into_df
from dags.utils.bigquery_utils import send_df_to_clickhouse
from airflow import DAG
from airflow.operators.python import PythonOperator
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from concurrent.futures import as_completed


session = requests.Session()
retries = Retry(total=10, backoff_factor=0.1,
                status_forcelist=[500, 502, 503, 504])
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))

data_source = "raw_tab"
# jurisdiction = "NSW"

meetings_list = []
races_list = []
pools_list = []
approx_list = []
legs_list = []
poolHistory_list = []
runners_list = []
ratings_list = []
betTypes_list = []
tips_list = []
lateMail_list = []
dividend_list = []
poolDividends_list = []
extraMarket_list = []
propisition_list = []
form_list = []
bigbet_list = []

exotic_pool_list = []
exotic_leg_list = []
scratchings_list = []

single_leg_premium_list = []
single_leg_premium_pool_trend_list = []
single_leg_premium_approx_list = []

jurisdiction_tqdm = []
meetingDate_tqdm = []
meeting_tqdm = []
race_tqdm = []

def process_meeting_task():
    access_token = get_access_token()

    executor = ThreadPoolExecutor(max_workers=10)

    # date = (datetime.today()-timedelta(days=1)).strftime("%Y-%m-%d")

    start_date = '2022-01-01'
    end_date = '2023-01-01'
    # start_date = '2023-10-01'
    # end_date = '2023-10-14'
    dates = pd.date_range(start=start_date, end=end_date)
    for date in dates:

        date = date.strftime("%Y-%m-%d")

        race_type = "racing"

        for jurisdiction in ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]:
            print(jurisdiction)
            meetings = get_json_from_api(f"https://api.beta.tab.com.au/v1/historical-results-service/{jurisdiction}/{race_type}/{date}", params=None, headers={'Authorization': f'Bearer {access_token}'})


            for meeting in meetings.get('meetings', []) if meetings is not None else []:
                
                meeting_raw = get_json_from_api(f"https://api.beta.tab.com.au/v1/historical-results-service/{jurisdiction}/racing/{date}/{meeting.get('venueMnemonic','')}/{meeting.get('raceType', '')}", params=None, headers={'Authorization': f'Bearer {access_token}'})
                
                
                if meeting_raw is not None:
                    meetings_list.append({
                        'meetingName' : meeting_raw.get('meetingName', np.nan),
                        'location' : meeting_raw.get('location', np.nan),
                        'venueMnemonic' : meeting_raw.get('venueMnemonic', np.nan),
                        'raceType' : meeting_raw.get('raceType', np.nan),
                        'meetingDate' : meeting_raw.get('meetingDate', np.nan),
                        'weatherCondition' : meeting_raw.get('weatherCondition', np.nan),
                        'trackCondition' : meeting_raw.get('trackCondition', np.nan),
                        'sellCode_meetingCode' : meeting_raw.get('sellCode', {}).get('meetingCode', np.nan),
                        'sellCode_scheduledType' : meeting_raw.get('sellCode', {}).get('scheduledType', np.nan),
                        'jurisdiction' : jurisdiction,
                    })

                for exoticPool in meeting_raw.get('exoticPools', []) if meeting_raw is not None else []:
                    
                    exotic_pool_list.append({
                        'poolStatusCode' : exoticPool.get('poolStatusCode', np.nan),
                        'wageringProduct' : exoticPool.get('wageringProduct', np.nan),
                        'poolCloseTime' : exoticPool.get('poolCloseTime', np.nan),
                        'mergePool' : exoticPool.get('mergePool', np.nan),
                        'mergePoolTotal' : exoticPool.get('mergePoolTotal', np.nan),
                        'poolTotal' : exoticPool.get('poolTotal', np.nan),
                        'jackpot' : exoticPool.get('jackpot', np.nan),
                        'meetingName' : meeting_raw.get('meetingName', np.nan),
                        'location' : meeting_raw.get('location', np.nan),
                        'venueMnemonic' : meeting_raw.get('venueMnemonic', np.nan),
                        'raceType' : meeting_raw.get('raceType', np.nan),
                        'meetingDate' : meeting_raw.get('meetingDate', np.nan),
                        'jurisdiction' : jurisdiction,
                    })

                    for leg in exoticPool.get('legs', []) if exoticPool is not None else []:
                        exotic_leg_list.append({
                            'legNumber' : leg.get('legNumber', np.nan),
                            'raceNumber' : leg.get('raceNumber', np.nan),
                            'venueMnemonic' : leg.get('venueMnemonic', np.nan),
                            'raceType' : leg.get('raceType', np.nan),
                            'startTime' : leg.get('startTime', np.nan),
                            'meetingName' : meeting_raw.get('meetingName', np.nan),
                            'location' : meeting_raw.get('location', np.nan),
                            'raceType' : meeting_raw.get('raceType', np.nan),
                            'meetingDate' : meeting_raw.get('meetingDate', np.nan),
                            'jurisdiction' : jurisdiction,
                        })

                for race in meeting_raw.get('races', []) if meeting_raw is not None else []:
                    race_tqdm.append(executor.submit(process_race, meeting_raw, race_type, date, race, jurisdiction, access_token)) 

                
    for races in tqdm(as_completed(race_tqdm), total=len(race_tqdm), desc="Processing Races"):
        pass

    
    

   
    # meetings_list
    meetingsDF = pd.DataFrame(meetings_list)
    data_name = "historical_meetings"
    table_name = f"{data_source}.{data_name}"
    send_df_to_clickhouse(meetingsDF, table_name)

    # exotic_pool_list
    exoticPoolDF = pd.DataFrame(exotic_pool_list)
    data_name = "historical_exoticPools"
    table_name = f"{data_source}.{data_name}"
    send_df_to_clickhouse(exoticPoolDF, table_name)

    # exotic_leg_list
    exoticLegDF = pd.DataFrame(exotic_leg_list)
    data_name = "historical_exoticLegs"
    table_name = f"{data_source}.{data_name}"
    send_df_to_clickhouse(exoticLegDF, table_name)

    # races_list
    racesDF = pd.DataFrame(races_list)
    data_name = "historical_races"
    table_name = f"{data_source}.{data_name}"
    send_df_to_clickhouse(racesDF, table_name)

    # scratchings_list
    scratchingsDF = pd.DataFrame(scratchings_list)
    data_name = "historical_scratchings"
    table_name = f"{data_source}.{data_name}"
    send_df_to_clickhouse(scratchingsDF, table_name)

    # runners_list
    runnersDF = pd.DataFrame(runners_list)

    column_types = {
        'runnerName' : str,
                            'runnerNumber' : int,
                            'fixedOdds_returnWin' : float,
                            'fixedOdds_returnWinOpen' : float,
                            'fixedOdds_returnPlace' : float,
                            'fixedOdds_bettingStatus' : str,
                            'fixedOdds_winDeduction' : float,
                            'fixedOdds_placeDeduction' : float,
                            'fixedOdds_propositionNumber' : float,
                            'fixedOdds_scratchedTime' : str,
                            'parimutuel_returnWin' : int,
                            'parimutuel_returnPlace' : int,
                            'parimutuel_bettingStatus' : str,
                            'trainerName' : str,
                            'barrierNumber' : int,
                            'riderDriverName' : str,
                            'finishingPosition' : int,
                            'claimAmount' : float,

                            'meeting_meetingName' : str,
                            'meeting_venueMnemonic' : str,
                            'meeting_meetingDate' : str,
                            'meeting_location' : str,
                            'meeting_raceType' : str,
                            'race_raceNumber' : int,
                            'race_raceName' : str,
                            'jurisdiction' : str,

    }

    for col, dtype in column_types.items():
        if col in runnersDF.columns:
            runnersDF[col] = runnersDF[col].astype(dtype)
        else:
            # Add new column with NaN values or suitable default values
            default_value = 0 if dtype == int else None
            if dtype == bool:
                default_value = False
            runnersDF[col] = default_value

    data_name = "historical_runners"
    table_name = f"{data_source}.{data_name}"
    send_df_to_clickhouse(runnersDF, table_name)

    # dividend_list
    dividendDF = pd.DataFrame(dividend_list)
    data_name = "historical_dividends"
    table_name = f"{data_source}.{data_name}"
    send_df_to_clickhouse(dividendDF, table_name)

    # poolDividends_list
    poolDividendsDF = pd.DataFrame(poolDividends_list)
    data_name = "historical_poolDividends"
    table_name = f"{data_source}.{data_name}"
    send_df_to_clickhouse(poolDividendsDF, table_name)

        

    print("finished process meeting")




def process_race(meeting_raw, race_type, date, race, jurisdiction, access_token):
    
                races_raw = get_json_from_api(f"https://api.beta.tab.com.au/v1/historical-results-service/{jurisdiction}/{race_type}/{date}/{meeting_raw.get('venueMnemonic','')}/{meeting_raw.get('raceType', '')}/races/{race.get('raceNumber', '')}", params=None, headers={'Authorization': f'Bearer {access_token}'})

                races_list.append({
                    'raceNumber' : races_raw.get('raceNumber', np.nan),
                    'raceName' : races_raw.get('raceName', np.nan),
                    'raceStatus' : races_raw.get('raceStatus', np.nan),
                    'raceStartTime' : races_raw.get('raceStartTime', np.nan),
                    'raceDistance' : races_raw.get('raceDistance', np.nan),
                    'willHaveFixedOdds' : races_raw.get('willHaveFixedOdds', np.nan),
                    'hasFixedOdds' : races_raw.get('hasFixedOdds', np.nan),
                    'results' : races_raw.get('results', np.nan),
                    'skyRacing_audio' : races_raw.get('skyRacing', {}).get('audio', np.nan),
                    'skyRacing_video' : races_raw.get('skyRacing', {}).get('video', np.nan),
                    'resultedTime' : races_raw.get('resultedTime', np.nan),
                    'substitute' : races_raw.get('substitute', np.nan),
                    'raceClassConditions' : races_raw.get('raceClassConditions', np.nan),
                    'hasParimutuel' : races_raw.get('hasParimutuel', np.nan),
                    'meetingName' : meeting_raw.get('meetingName', np.nan),
                    'location' : meeting_raw.get('location', np.nan),
                    'venueMnemonic' : meeting_raw.get('venueMnemonic', np.nan),
                    'raceType' : meeting_raw.get('raceType', np.nan),
                    'meetingDate' : meeting_raw.get('meetingDate', np.nan),
                    'jurisdiction' : jurisdiction,

                })

                for scratching in races_raw.get('scratchings', []) if races_raw is not None else []:
                    scratchings_list.append({
                            'runnerName' : scratching.get('runnerName', np.nan),
                            'runnerNumber' : scratching.get('runnerNumber', np.nan),
                            'fixedOdds_returnWin' : scratching.get('fixedOdds', {}).get('returnWin', np.nan),
                            'fixedOdds_returnWinOpen' : scratching.get('fixedOdds', {}).get('returnWinOpen', np.nan),
                            'fixedOdds_returnPlace' : scratching.get('fixedOdds', {}).get('returnPlace', np.nan),
                            'fixedOdds_bettingStatus' : scratching.get('fixedOdds', {}).get('bettingStatus', np.nan),
                            'fixedOdds_winDeduction' : scratching.get('fixedOdds', {}).get('winDeduction', np.nan),
                            'fixedOdds_placeDeduction' : scratching.get('fixedOdds', {}).get('placeDeduction', np.nan),
                            'fixedOdds_propositionNumber' : scratching.get('fixedOdds', {}).get('propositionNumber', np.nan),
                            'fixedOdds_scratchedTime' : scratching.get('fixedOdds', {}).get('scratchedTime', np.nan),
                            'parimutuel_returnWin' : scratching.get('parimutuel', {}).get('returnWin', np.nan),
                            'parimutuel_returnPlace' : scratching.get('parimutuel', {}).get('returnPlace', np.nan),
                            'parimutuel_bettingStatus' : scratching.get('parimutuel', {}).get('bettingStatus', np.nan),
                            'trainerName' : scratching.get('trainerName', np.nan),
                            'barrierNumber' : scratching.get('barrierNumber', np.nan),
                            'riderDriverName' : scratching.get('riderDriverName', np.nan),
                            'finishingPosition' : scratching.get('finishingPosition', np.nan),
                            'claimAmount' : scratching.get('claimAmount', np.nan),

                            'meeting_meetingName' : races_raw.get('meeting', {}).get('meetingName', np.nan),
                            'meeting_venueMnemonic' : races_raw.get('meeting', {}).get('venueMnemonic', np.nan),
                            'meeting_meetingDate' : races_raw.get('meeting', {}).get('meetingDate', np.nan),
                            'meeting_location' : races_raw.get('meeting', {}).get('location', np.nan),
                            'meeting_raceType' : races_raw.get('meeting', {}).get('raceType', np.nan),
                            'race_raceNumber' : races_raw.get('raceNumber', np.nan),
                            'race_raceName' : races_raw.get('raceName', np.nan),
                            'jurisdiction' : jurisdiction,
                        })

                for runner in races_raw.get('runners', []) if races_raw is not None else []:
                    runners_list.append({
                            'runnerName' : runner.get('runnerName', np.nan),
                            'runnerNumber' : runner.get('runnerNumber', np.nan),
                            'fixedOdds_returnWin' : runner.get('fixedOdds', {}).get('returnWin', np.nan),
                            'fixedOdds_returnWinOpen' : runner.get('fixedOdds', {}).get('returnWinOpen', np.nan),
                            'fixedOdds_returnPlace' : runner.get('fixedOdds', {}).get('returnPlace', np.nan),
                            'fixedOdds_bettingStatus' : runner.get('fixedOdds', {}).get('bettingStatus', np.nan),
                            'fixedOdds_winDeduction' : runner.get('fixedOdds', {}).get('winDeduction', np.nan),
                            'fixedOdds_placeDeduction' : runner.get('fixedOdds', {}).get('placeDeduction', np.nan),
                            'fixedOdds_propositionNumber' : runner.get('fixedOdds', {}).get('propositionNumber', np.nan),
                            'fixedOdds_scratchedTime' : runner.get('fixedOdds', {}).get('scratchedTime', np.nan),
                            'parimutuel_returnWin' : runner.get('parimutuel', {}).get('returnWin', np.nan),
                            'parimutuel_returnPlace' : runner.get('parimutuel', {}).get('returnPlace', np.nan),
                            'parimutuel_bettingStatus' : runner.get('parimutuel', {}).get('bettingStatus', np.nan),
                            'trainerName' : runner.get('trainerName', np.nan),
                            'barrierNumber' : runner.get('barrierNumber', np.nan),
                            'riderDriverName' : runner.get('riderDriverName', np.nan),
                            'finishingPosition' : runner.get('finishingPosition', np.nan),
                            'claimAmount' : runner.get('claimAmount', np.nan),

                            'meeting_meetingName' : races_raw.get('meeting', {}).get('meetingName', np.nan),
                            'meeting_venueMnemonic' : races_raw.get('meeting', {}).get('venueMnemonic', np.nan),
                            'meeting_meetingDate' : races_raw.get('meeting', {}).get('meetingDate', np.nan),
                            'meeting_location' : races_raw.get('meeting', {}).get('location', np.nan),
                            'meeting_raceType' : races_raw.get('meeting', {}).get('raceType', np.nan),
                            'race_raceNumber' : races_raw.get('raceNumber', np.nan),
                            'race_raceName' : races_raw.get('raceName', np.nan),
                            'jurisdiction' : jurisdiction,
                        })
                    
                for dividend in races_raw.get('dividends', []) if races_raw is not None else []:
                        # dividends (array, but save to df)
                        dividend_list.append({
                            'wageringProduct' : dividend.get('wageringProduct', np.nan),
                            'jackpotCarriedOver' : dividend.get('jackpotCarriedOver', np.nan),
                            'mergePool' : dividend.get('mergePool', np.nan),
                            'poolStatusCode' : dividend.get('poolStatusCode', np.nan),
                            'mergePoolTotal' : dividend.get('mergePool', np.nan),
                            'poolCloseTime' : dividend.get('poolCloseTime', np.nan),

                            'meeting_meetingName' : races_raw.get('meeting', {}).get('meetingName', np.nan),
                            'meeting_venueMnemonic' : races_raw.get('meeting', {}).get('venueMnemonic', np.nan),
                            'meeting_meetingDate' : races_raw.get('meeting', {}).get('meetingDate', np.nan),
                            'meeting_location' : races_raw.get('meeting', {}).get('location', np.nan),
                            'meeting_raceType' : races_raw.get('meeting', {}).get('raceType', np.nan),
                            'race_raceNumber' : races_raw.get('raceNumber', np.nan),
                            'race_raceName' : races_raw.get('raceName', np.nan),
                            'jurisdiction' : jurisdiction,
                        })

                        # dividendDf = pd.DataFrame(
                        #     wageringProduct
                        #     name
                        #     tier
                        #     poolStatusCode
                        #     countBackLevelDescription
                        #     jackpotCarriedOver
                        # )

                        # poolDividendsDF = turn_json_into_df(race_full.get('poolDividends'])
                        for poolDividend in dividend.get('poolDividends', []):
                            poolDividends_list.append({
                                'wageringProduct' : dividend.get('wageringProduct', np.nan),
                                'meeting_meetingName' : races_raw.get('meeting', {}).get('meetingName', np.nan),
                                'meeting_venueMnemonic' : races_raw.get('meeting', {}).get('venueMnemonic', np.nan),
                                'meeting_meetingDate' : races_raw.get('meeting', {}).get('meetingDate', np.nan),
                                'meeting_location' : races_raw.get('meeting', {}).get('location', np.nan),
                                'meeting_raceType' : races_raw.get('meeting', {}).get('raceType', np.nan),
                                'race_raceNumber' : races_raw.get('raceNumber', np.nan),
                                'race_raceName' : races_raw.get('raceName', np.nan),
                                'amount' : poolDividend.get('amount', np.nan),
                                'selections' : poolDividend.get('selections', np.nan),
                                'jurisdiction' : jurisdiction,
                            })




# start code
if __name__ == "__main__":
    # run all functions
    process_meeting_task()
    
    