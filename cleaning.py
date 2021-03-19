#%%
import pandas as pd
import numpy as np

df_main = pd.read_csv('data.csv')
df_add = pd.read_csv('add_data.csv')

df_main.drop('Unnamed: 0', inplace = True)
df_add.drop('Unnamed: 0', inplace = True)

df_merged = pd.merge(df_main, df_add, how = 'left', on = ['resort_name', 'resort_name'])

print(df_merged)
# %%
print(df_add)
# %%
