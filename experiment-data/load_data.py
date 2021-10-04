import sqlite3 as sql
import pandas as pd
import matplotlib.pyplot as plt
from os import listdir
from os.path import isfile, join

data = "./csvs"
load_csv_names = [f for f in listdir(data) if isfile(join(data, f)) and f.endswith(".csv")]
csv_as_df = [pd.read_csv(join(data, csv)) for csv in load_csv_names]
conn = sql.connect('pvd.db')
for i in range(len(csv_as_df)):
	csv_as_df[i].to_sql(load_csv_names[i][:-4], conn, if_exists='replace')
