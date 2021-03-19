# Ski_scraper
A bot to scrape ski-resort-stats.com using Selenium webdriver and multi-threading.

## Requirements

Uses the Multi-Webbing module, install it with pip:

`pip install multi_webbing`

## Runtime!

1. Instance bot with:  `skiscraper = ski_scraper(n)`

    n is optional to limit the number of rows scraped, if left blank it will scrape the entire table.

2. Scrape the main table and collect urls:
    `skibot.scrape_table()`

3. Scrape the individual pages:
    `skibot.scrape_pages(threads = 3)`

Be kind to their servers and don't add too many threads.

Don't forget to have fun!