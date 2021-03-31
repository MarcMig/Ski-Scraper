
import pandas as pd
import numpy as np
import country_converter as coco

class Cleaner:

    def __init__(self):
        self.merged = pd.DataFrame()

    def loadandmerge(self):
        df_main = pd.read_csv('dataNone.csv', index_col=0)
        df_add = pd.read_csv('add_dataNone.csv', index_col=0)
        self.merged = pd.merge(df_main, df_add, how = 'left', on = ['resort_name', 'resort_name'])


    def childandcountry(self):
        ## Turn 'Child_friendly' to bool
        self.merged['child_friendly'].replace(to_replace=['Yes','No'],value=[True,False],inplace=True)

        ## Clean country, add ISO column
        self.merged['country'].replace(to_replace='Siberia',value='Russia',inplace=True)
        country_lst = self.merged['country'].tolist()
        self.merged['country_iso'] = coco.CountryConverter().convert(names = country_lst, to='ISO3')
        self.merged['altitude_diff'] = self.merged['max_altitude'] - self.merged['min_altitude']

    def snowgrooming(self):
        # Convert slopes to float
        self.merged['beginner_slopes'] = self.merged['beginner_slopes'].str.replace('km','')
        self.merged['beginner_slopes'] = self.merged['beginner_slopes'].str.replace(',','.')
        self.merged['beginner_slopes'].replace(to_replace='N/A ',inplace = True)
        self.merged['beginner_slopes'] = pd.to_numeric(self.merged['beginner_slopes'])

        self.merged['intermediate_slopes'] = self.merged['intermediate_slopes'].str.replace('km','')
        self.merged['intermediate_slopes'] = self.merged['intermediate_slopes'].str.replace(',','.')
        self.merged['intermediate_slopes'].replace(to_replace='N/A ',inplace = True)
        self.merged['intermediate_slopes'].replace(r'^\s*$', np.nan, regex=True,inplace = True)
        self.merged['intermediate_slopes'] = pd.to_numeric(self.merged['intermediate_slopes'])

        self.merged['difficult_slopes'] = self.merged['difficult_slopes'].str.replace('km','')
        self.merged['difficult_slopes'] = self.merged['difficult_slopes'].str.replace(',','.')
        self.merged['difficult_slopes'] = self.merged['difficult_slopes'].str.replace('[','')
        self.merged['difficult_slopes'].replace(to_replace='N/A ',inplace = True)
        self.merged['difficult_slopes'] = pd.to_numeric(self.merged['difficult_slopes'])

        # Add total slopes collumn
        self.merged['Total_slopes(km)'] = self.merged[['beginner_slopes','intermediate_slopes','difficult_slopes']].sum(axis=1)

    def liftsandcannons(self):
        # Convert T-lifts to float
        pd.to_numeric(self.merged['T-Bar_Lifts'],errors = 'coerce')

        # Convert Snow cannons to float
        self.merged['snow_cannons'].replace(to_replace='No',inplace = True)
        self.merged['snow_cannons'].replace(to_replace='no report',inplace = True)
        self.merged['snow_cannons'] = pd.to_numeric(self.merged['snow_cannons'])


    def season_splitter(self):
        # Divide season to start and end - filter out problematic lines
        rows = self.merged.loc[(
            (self.merged['season'] == 
            'December - April June - August October - November')
            |
            (self.merged['season'] == 
            'October - November December - May June - October')
            |
            (self.merged['season'] == 
            'November - May June - August')
            |
            (self.merged['season'] == 
            'no report')
            |
            (self.merged['season'] == 
            'Year-round')
            )]

        df_drop = self.merged.drop(index=rows.index)
        splits = pd.DataFrame()
        splits[['Season_start','Season_end']] = df_drop['season'].str.split(' - ',expand= True)
        self.merged = self.merged.merge(splits,how= 'left', left_index=True, right_index=True)

        # Hardcode stations with glacier skiing
        self.merged.loc[(self.merged['season'] == 
            'December - April June - August October - November'),['Season_start','Season_end']] = ['December','April']

        self.merged.loc[(self.merged['season'] == 
            'October - November December - May June - October'),['Season_start','Season_end']] = ['December','May']

        self.merged.loc[(self.merged['season'] == 
            'November - May June - August'),['Season_start','Season_end']] = ['November','May']



if __name__ == '__main__':

    squeeky = Cleaner()
    squeeky.loadandmerge()
    squeeky.childandcountry()
    squeeky.snowgrooming()
    squeeky.liftsandcannons()
    squeeky.season_splitter()
    squeeky.merged.to_csv('shiny_data.csv')