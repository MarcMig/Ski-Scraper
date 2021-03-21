# Ski_scraper
A bot to scrape ski-resort-stats.com and collect general info on Ski resorts worldwide.

Basic Bot template provided by [Harry Berg](https://github.com/life-efficient)

## Requirements

Uses Selenium webdriver and the Multi-Webbing module courtesy of [Adam Hardy](https://github.com/adhardy), install it with pip:

`pip install multi_webbing`

## Runtime!

1. Instance bot with:  `skiscraper = ski_scraper(n)`

    n is optional to limit the number of rows scraped, if left blank it will scrape the entire table.

2. Scrape the main [table](https://ski-resort-stats.com/find-ski-resort/) and collect urls:
    `skibot.scrape_table()`
    The rows are added to a DataFrame and the result is saved locally as 'data.csv'

3. Scrape the [individual pages](https://ski-resort-stats.com/Hemsedal/):
    `skibot.scrape_pages(threads = 2)`
    
    Be kind to their servers and don't add too many threads.
    The function selects the Urls previously scraped from the main table and loops through them scraping the individual pages for additional data as well as historical snow depths.
    The additional data is saved locally as add_data.csv and the snow depth data as snow_data.json.

4. Initial data cleaning 

    Run `cleaning.py` to merge the datasets and clean the merged data. 

Don't forget to have fun!