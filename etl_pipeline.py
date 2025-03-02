import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3

# Step 1: Extract - Web Scraping Yelp (Example for Market Expansion)
def scrape_yelp_data(city, category="restaurants"):
    url = f"https://www.yelp.com/search?find_desc={category}&find_loc={city}"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        businesses = soup.find_all("h3", class_="css-1egxyvc")
        data = [{"Business Name": biz.text} for biz in businesses]
        return pd.DataFrame(data)
    else:
        print("Failed to retrieve data from Yelp")
        return pd.DataFrame()

# Step 2: Extract - API Example (World Bank Economic Data)
def fetch_world_bank_data(indicator="NY.GDP.MKTP.CD", country="US", year="2022"):
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()[1][0]
        return pd.DataFrame([{"Country": data["country"]["value"], "GDP ($)": data["value"], "Year": data["date"]}])
    else:
        print("Failed to retrieve World Bank data")
        return pd.DataFrame()

# Step 3: Transform - Data Cleaning & Formatting
def clean_data(df):
    df.dropna(inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

# Step 4: Load - Store Data in SQLite Database
def load_to_sqlite(df, table_name, db_name="market_expansion.db"):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data successfully loaded into {table_name} table.")

# Running the ETL Pipeline
if __name__ == "__main__":
    # Scrape Yelp data for a city (e.g., Houston)
    yelp_data = scrape_yelp_data("Houston")
    yelp_data = clean_data(yelp_data)
    load_to_sqlite(yelp_data, "yelp_businesses")

    # Fetch World Bank GDP data for the US
    gdp_data = fetch_world_bank_data()
    gdp_data = clean_data(gdp_data)
    load_to_sqlite(gdp_data, "world_bank_gdp")

    print("ETL Pipeline Completed Successfully!")
