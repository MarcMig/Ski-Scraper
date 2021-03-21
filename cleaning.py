#%%
import pandas as pd
import numpy as np
import country_converter as coco


df_main = pd.read_csv('data.csv').drop('Unnamed: 0', axis=1)
df_add = pd.read_csv('add_data.csv').drop('Unnamed: 0', axis=1)

df_merged = pd.merge(df_main, df_add, how = 'left', on = ['resort_name', 'resort_name'])

## Turn 'Child_friendly' to bool
df_merged['child_friendly'].replace(to_replace=['Yes','No'],value=[True,False],inplace=True)

## Clean country, add ISO column
df_merged['country'].replace(to_replace='Siberia',value='Russia',inplace=True)
country_lst = df_merged['country'].tolist()
df_merged['country_iso'] = coco.CountryConverter().convert(names = country_lst, to='ISO3')
df_merged['altitude_diff'] = df_merged['max_altitude'] - df_merged['min_altitude']

# Convert slopes to float
df_merged['Beginner_slopes(km)'] = df_merged['Beginner_slopes(km)'].str.replace('km','')
df_merged['Beginner_slopes(km)'] = df_merged['Beginner_slopes(km)'].str.replace(',','.')
df_merged['Beginner_slopes(km)'].replace(to_replace='N/A ',inplace = True)
df_merged['Beginner_slopes(km)'] = pd.to_numeric(df_merged['Beginner_slopes(km)'])

df_merged['Intermediate_slopes(km)'] = df_merged['Intermediate_slopes(km)'].str.replace('km','')
df_merged['Intermediate_slopes(km)'] = df_merged['Intermediate_slopes(km)'].str.replace(',','.')
df_merged['Intermediate_slopes(km)'].replace(to_replace='N/A ',inplace = True)
df_merged['Intermediate_slopes(km)'].replace(r'^\s*$', np.nan, regex=True,inplace = True)
df_merged['Intermediate_slopes(km)'] = pd.to_numeric(df_merged['Intermediate_slopes(km)'])

df_merged['Difficult_slopes(km)'] = df_merged['Difficult_slopes(km)'].str.replace('km','')
df_merged['Difficult_slopes(km)'] = df_merged['Difficult_slopes(km)'].str.replace(',','.')
df_merged['Difficult_slopes(km)'] = df_merged['Difficult_slopes(km)'].str.replace('[','')
df_merged['Difficult_slopes(km)'].replace(to_replace='N/A ',inplace = True)
df_merged['Difficult_slopes(km)'] = pd.to_numeric(df_merged['Difficult_slopes(km)'])

# Add total slopes collumn
df_merged['Total_slopes(km)'] = df_merged[['Beginner_slopes(km)','Intermediate_slopes(km)','Difficult_slopes(km)']].sum(axis=1)

# Convert T-lifts to float
pd.to_numeric(df_merged['T-Bar_Lifts'],errors = 'coerce')

# Convert Snow cannons to float
df_merged['Snow_cannons'].replace(to_replace='No',inplace = True)
df_merged['Snow_cannons'].replace(to_replace='no report',inplace = True)
df_merged['Snow_cannons'] = pd.to_numeric(df_merged['Snow_cannons'])

# Divide season to start and end
rows = df_merged.loc[(
    (df_merged['season'] == 
    'December - April June - August October - November')
    |
    (df_merged['season'] == 
    'October - November December - May June - October')
    |
    (df_merged['season'] == 
    'November - May June - August')
    |
    (df_merged['season'] == 
    'no report')
    |
    (df_merged['season'] == 
    'Year-round')
    )]

df_drop = df_merged.drop(index=rows.index)
splits = pd.DataFrame()
splits[['Season_start','Season_end']] = df_drop['season'].str.split(' - ',expand= True)
df_d_merged = df_merged.merge(splits,how= 'left', left_index=True, right_index=True)
df_d_merged.loc[(df_d_merged['season'] == 
    'December - April June - August October - November')]['Season_start','Season_end'] = ['December','April']
# %%
df_d_merged.describe()
# %%
df_d_merged.to_csv('Shiny_Data.csv')
# %%
