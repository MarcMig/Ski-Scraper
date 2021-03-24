from bot import Bot
import pandas as pd
from time import sleep
import json
from multi_webbing import multi_webbing as mw
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import presence_of_element_located

class SkiScraper(Bot):
    '''
    Create a scrapper Bot!

    Args:
        n (int): used to limit the number of rows scraped for testing

    Attributes:
        df (DataFrame): stores the scraped data
        n (int): stores the scrape limiter

    Methods:
        scrape_table: scrapes the main table and stores data in df, 
                      run first to gather the urls of the station pages

        scrape_pages(threads): scrapes individual station pages using
                      using the number of threads specified, since the
                      website is a beta version using more than 2 might
                      make it skip pages

    '''
    
    def __init__(self, n = 'all'):

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

        if self.n == 'all':
            table_length = int(self.driver.find_element_by_xpath('//*[@id="table_1_info"]').text.split()[3])
        else:
            table_length = self.n

        # Scrape table, save data to csv file

        for i in range(table_length):    

            item = f'//*[@id="table_21_row_{i}"]/td'
            row_items_list = self.driver.find_elements_by_xpath(item)
            ex = { 'resort_name' : row_items_list[0].text,
            'url' : row_items_list[0].find_element_by_tag_name('a').get_attribute('href'),
            'continent' : row_items_list[1].text,
            'country' : row_items_list[2].text,
            'max_altitude' : (float(row_items_list[3].text)*1000 if float(row_items_list[3].text) < 10 else float(row_items_list[3].text)), 
            'min_altitude' : (float(row_items_list[4].text)*1000 if float(row_items_list[4].text) < 10 else float(row_items_list[4].text)),
            'child_friendly' : row_items_list[5].text,
            'ski_pass_price' : row_items_list[6].text,
            'season' : row_items_list[7].text }

            self.df = self.df.append(ex, ignore_index=True)
            self.df.to_csv(f'data_{self.n}.csv')

            if self.verbose:
              print('Got info for:' + row_items_list[0].text)

        # Close driver

        self.driver.quit()

    def scrape_pages(self, threads = 1):

        # Get list of resorts and Urls from DataFrame
        extra_data_df = []
        snow_df = {}
        resorts = self.df[self.df['url'] != 'https://ski-resort-stats.com/beta-project'][['url' , 'resort_name']]

        if self.n != 'all': resorts = resorts.iloc[:self.n,]

        # Initiate threads

        num_threads = threads
        my_threads = mw.MultiWebbing(num_threads, web_module="selenium")
        my_threads.start()


        #  Cannot append to Pandas in Job function so worker functions are defined
        #  to create dataFrame and save data  

        def add_to_data():
            df_data = pd.DataFrame(extra_data_df)
            df_data.to_csv(f'add_data_{self.n}.csv')
        
        def add_snow():
            df_snow = pd.DataFrame.from_dict(snow_df, orient= 'index')
            df_snow.to_json(f'snow_data_{self.n}.json')

        # Job function for multi-threading 

        def get_ski_info(job):
            
            # Retrieve local vars

            resort = job.custom_data[0]
            extra_data_df = job.custom_data[1]
            snow_df = job.custom_data[2]

            # Go to webpage, find elements

            job.get_url()
            sleep(1)

            dta = job.driver.find_elements_by_xpath('//*/table/tbody/tr')
            data = {}
            renderData = {}

            items_list = []
            for item in dta:
                row = item.find_elements_by_xpath('td/span')
                for ele in row:
                    items_list.append(ele)
            
            # Check page isnt blank

            if len(items_list) == 0:
                if self.verbose: print('No data for: ' + resort)

            else:
                # Check if snow depth chart is present, scrape it! 
                try: 
                    scpt = job.driver.execute_script('return JSON.stringify(wpDataCharts)')
                except:
                    if self.verbose: print('No snow data for: ' + resort)
                else:
                    js_script = json.loads(scpt)


                    for row in js_script:
                        renderData['options'] = js_script[row]['render_data']['options']
                    
                    weeks = renderData['options']['xAxis']['categories']

                    for item in renderData['options']['series']:
                        data[item['name']] = list(zip(item['data'], weeks))

                extra_data = {  'resort_name': resort,
                        'Beginner_slopes(km)': items_list[3].text,
                        'Intermediate_slopes(km)': items_list[4].text,
                        'Difficult_slopes(km)': items_list[5].text,
                        'T-Bar_Lifts': items_list[6].text,
                        'Chairlifts': items_list[7].text,
                        'Gondolas': items_list[8].text,
                        'Snowpark': items_list[9].text == 'Yes',
                        'Night_skiing': items_list[10].text == 'Yes',
                        'Snow_cannons': items_list[11].text                            
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

        # While loop adds 
        while my_threads.job_queue.qsize() > 0:
            sleep(1)
            if self.verbose: print(my_threads.job_queue.qsize())

        my_threads.finish()


if __name__ == '__main__':
    
    skibot = ski_scrapper()
    skibot.verbose = True
    skibot.scrape_table()
    skibot.scrape_pages(threads = 2)