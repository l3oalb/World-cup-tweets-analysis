import pandas as pd
import os

DIR = os.path.dirname(os.path.realpath(__file__))
WC_RESULTS_DIR = os.path.join(DIR, "wc2018_results_data")

# Chargement du CSV : Goals
CSV_PATH_GOALS = os.path.join(WC_RESULTS_DIR, "World_cup_2018_goals.csv")
df_goals = pd.read_csv(CSV_PATH_GOALS)

# Chargement du CSV : Country
CSV_PATH_COUNTRY = os.path.join(WC_RESULTS_DIR, "World_cup_2018_country.csv")
df_country = pd.read_csv(CSV_PATH_COUNTRY)

# Chargement du CSV : Matches
CSV_PATH_MATCHES = os.path.join(WC_RESULTS_DIR, "World_cup_2018_matches.csv")
df_matches = pd.read_csv(CSV_PATH_MATCHES)

