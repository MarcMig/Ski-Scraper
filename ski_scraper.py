from bot import Bot
import pandas as pd
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

if __name__ == '__main__':
    
    skibot = ski_scrapper()
    skibot.verbose = True
    skibot.scrape_table(10)
    skibot.driver.quit()