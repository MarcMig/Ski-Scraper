from bot import Bot
import pandas as pd
from time import sleep
import requests
import json
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import presence_of_element_located

class ski_scrapper(Bot):
    
    def __init__(self):

        super().__init__()
        self.df = pd.DataFrame()

    def scrape_table(self, rows = None):
        
        self.driver.get('https://ski-resort-stats.com/find-ski-resort/')
        wait = WebDriverWait(self.driver, 10)
        wait.until(presence_of_element_located((By.ID,'table_21_row_0')))

        self.scroll(y = 2000)
        self.driver.find_element_by_xpath('//*[@id="table_1_length"]/label/div/select/option[7]').click()

        if rows == None:
            table_length = int(self.driver.find_element_by_xpath('//*[@id="table_1_info"]').text.split()[3])
        else:
            table_length = rows

        for i in range(table_length):    
            item = f'//*[@id="table_21_row_{i}"]/td'
            lst = self.driver.find_elements_by_xpath(item)
            ex = { 'resort_name' : lst[0].text,
            'url' : lst[0].find_element_by_tag_name('a').get_attribute('href'),
            'continent' : lst[1].text,
            'country' : lst[2].text,
            'max_altitude' : (float(lst[3].text)*1000 if float(lst[3].text) < 10 else float(lst[3].text)), 
            'min_altitude' : (float(lst[4].text)*1000 if float(lst[4].text) < 10 else float(lst[4].text)),
            'child_friendly' : lst[5].text,
            'ski_pass_price' : lst[6].text,
            'season' : lst[7].text }

            self.df = self.df.append(ex, ignore_index=True)
            self.df.to_csv('data12.csv')

            if self.verbose:
              print('Got info for:' + lst[0].text)

    def scrape_pages(self, n = None):

        extra_data_df = pd.DataFrame()
        snow_df = pd.DataFrame()
        resorts = self.df[self.df['url'] != 'https://ski-resort-stats.com/beta-project'][['url' , 'resort_name']]

        if n != None: resorts = resorts.iloc[:n,]

        for url,resort in [tuple(x) for x in resorts.to_numpy()]:
    
        # Check page response
            r = requests.get(url)

            if r.status_code == 200:
                self.driver.get(url)
                sleep(1)

                dta = self.driver.find_elements_by_xpath('//*/table/tbody/tr')
                data = {}
                renderData = {}

                l = []
                for item in dta:
                    k = item.find_elements_by_xpath('td/span')
                    for i in k:
                        l.append(i)
                
                # Check page isnt blank

                if len(l) == 0:
                    if self.verbose: print('No data for: ' + resort)

                else:
                    # Check if snow depth chart is present 
                    try: 
                        scpt = self.driver.execute_script('return JSON.stringify(wpDataCharts)')
                    except:
                        if self.verbose: print('No snow data for: ' + resort)
                    else:
                        x = json.loads(scpt)

                        for k in x:
                            renderData['options'] = x[k]['render_data']['options']
                        

                        for item in renderData['options']['series']:
                            data[item['name']] = item['data']
                    
                        data['weeks'] = renderData['options']['xAxis']['categories']

                    extra_data = {  'resort_name': resort,
                            'Beginner_slopes(km)': l[3].text,
                            'Intermediate_slopes(km)': l[4].text,
                            'Difficult_slopes(km)': l[5].text,
                            'T-Bar_Lifts': l[6].text,
                            'Chairlifts': l[7].text,
                            'Gondolas': l[8].text,
                            'Snowpark': l[9].text == 'Yes',
                            'Night_skiing': l[10].text == 'Yes',
                            'Snow_cannons': l[11].text                            
                                }

                    snow_data = { 'resort_name': resort,
                                    'Snow_data': data 
                                }
                               
                    extra_data_df = extra_data_df.append(extra_data, ignore_index=True)
                    self.df = pd.merge(self.df, extra_data_df, how = 'left', on = ['resort_name','resort_name'])
                    self.df.to_csv('merged_data.csv')

                    snow_df = snow_df.append(snow_data, ignore_index=True)
                    snow_df.to_json('Snow_data')

                    if self.verbose: print('Got info for: ' + resort)
            else:
                if self.verbose: print('No info for: ' + resort)

if __name__ == '__main__':
    
    skibot = ski_scrapper()
    skibot.verbose = True
    skibot.scrape_table(10)
    skibot.scrape_pages(10)
    skibot.driver.quit()