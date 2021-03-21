from bot import Bot
import pandas as pd
from time import sleep
import json
from multi_webbing import multi_webbing as mw
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import presence_of_element_located

class ski_scrapper(Bot):
    
    def __init__(self, n = None):

        super().__init__()
        self.df = pd.DataFrame()
        self.n = n

    def scrape_table(self):
        
        # Initiate driver, get website and wait for table to load

        self.driver.get('https://ski-resort-stats.com/find-ski-resort/')
        wait = WebDriverWait(self.driver, 10)
        wait.until(presence_of_element_located((By.ID,'table_21_row_0')))

        # Expand table to include all rows for easy scraping

        self.scroll(y = 2000)
        self.driver.find_element_by_xpath('//*[@id="table_1_length"]/label/div/select/option[7]').click()

        # Restrict rows to scrape through n or find table length on website

        if self.n == None:
            table_length = int(self.driver.find_element_by_xpath('//*[@id="table_1_info"]').text.split()[3])
        else:
            table_length = self.n

        # Scrape table, save data to csv file

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
            self.df.to_csv('data.csv')

            if self.verbose:
              print('Got info for:' + lst[0].text)

        self.driver.quit()

    def scrape_pages(self, threads = 1):

        extra_data_df = []
        snow_df = {}
        resorts = self.df[self.df['url'] != 'https://ski-resort-stats.com/beta-project'][['url' , 'resort_name']]

        if self.n != None: resorts = resorts.iloc[:self.n,]

        #Init threads
        num_threads = threads
        my_threads = mw.MultiWebbing(num_threads, web_module="selenium")
        my_threads.start()

        def add_to_data():
            df_data = pd.DataFrame(extra_data_df)
            df_data.to_csv('add_data.csv')
        
        def add_snow():
            df_snow = pd.DataFrame.from_dict(snow_df, orient= 'index')
            df_snow.to_json('snow_data.json')


        def get_ski_info(job):
        
            resort = job.custom_data[0]
            extra_data_df = job.custom_data[1]
            snow_df = job.custom_data[2]

            job.get_url()
            sleep(1)

            dta = job.driver.find_elements_by_xpath('//*/table/tbody/tr')
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
                    scpt = job.driver.execute_script('return JSON.stringify(wpDataCharts)')
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
                
                # Append data to lists and Save

                job.lock.acquire()

                extra_data_df.append(extra_data) 
                add_to_data()

                if data != {}:
                    snow_df[resort] = data
                    add_snow()

                job.lock.release()

                if self.verbose: print('Got info for: ' + resort)

        # Loop adds jobs to queue
        for url,resort in [tuple(x) for x in resorts.to_numpy()]:
            my_threads.job_queue.put(mw.Job(get_ski_info, url, (resort, extra_data_df, snow_df)))

        while my_threads.job_queue.qsize() > 0:
            sleep(1)
            if self.verbose: print(my_threads.job_queue.qsize())

        my_threads.finish()


if __name__ == '__main__':
    
    skibot = ski_scrapper()
    skibot.verbose = True
    skibot.scrape_table()
    skibot.scrape_pages(threads = 2)