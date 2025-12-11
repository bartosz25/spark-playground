import pandas as pd
RED = "\033[31m"
RESET = "\033[0m"
matchdays_df = pd.DataFrame([
    {'matchday': 1, 'date': '2025-10-10'}, {'matchday': 2, 'date': '2025-10-17'},
    {'matchday': 3, 'date': '2025-10-24'}, {'matchday': 4, 'date': '2025-10-31'}
])
scorers_df = pd.DataFrame([
    {'matchday': 1, 'player': 'Joe Doe', 'goals': 2}, {'matchday': 3, 'player': 'Joe Doe', 'goals': 4},
])

for direction in ['forward', 'backward', 'nearest']:
    print(f'{RED}Direction: {direction}{RESET}')
    merged = pd.merge_asof(matchdays_df, scorers_df, on='matchday', direction=direction)
    print(merged.head(), end='\n\n')
